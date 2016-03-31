
namespace SPContrib.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Xml.Linq;
    using System.Xml.XPath;
    using Extranetpro.Common;
    using Extranetpro.Common.Configuration.Types;
    using Microsoft.Office.Server.Search.Administration;
    using Microsoft.SharePoint;
    using Microsoft.SharePoint.Administration;
    using Be.Axa.Shared.SharePoint.Core.Shared.Helpers;

    //TODO: Extract all strings from SearchAdminUtility and put them in the Configuration class.
    //TODO: Split this class in two to separate ContentSource functions and MetadataProperty functions.
    public class SearchAdminUtility
    {
        private const string ContentSourceNotFoundRemovalFailedLogFormat = "Removing content source failed. No content source found with the name \"{0}\".";
        private const string DefaultSsaProxyNotFoundExceptionMessage = "Could not find default Search Service Application Proxy assigned to the current web application";
        private const string CreateCrawledPropertyExceptionFormat = "Exception has been thrown while creating Crawled Property {0} . {1}";
        private const string PropertyMappingFailedMutipleMatchLogFormat = "Multiple match found for crawled property \"{0}\" in category \"{1}\". There are {2} property matching this name in the category.";
        private const string PropertyMappingFailedNotFoundLogFormat = "Crawled property was not found. There is no crawled property \"{0}\" in category \"{1}\".";
        private const string PropertyMappingFailedExceptionFormat = "Could not map managed property {0}. It can be due to invalid mapping in search configuration XML. Please read the previous critical message in the application logs for more information.";
        private const string ContentSourceExistsLogFormat = "Content source \"{0}\" will not be created as it already exists";
        private const string ContentSourceNotSupportedExceptionFormat = "Content source of type \"{0}\" is not supported.";
        private const string CreateManagedPropertyExceptionFormat = "Exception has been thrown while creating Managed Property {0} . {1}";
        private const string KnownManagedPropertyAttributeMissingLogFormat = "Managed property [Name={0}] defined in search configuration XML, is missing \"{1}\" attribute.";
        private const string UnknownManagedPropertyAttributeMissingLogFormat = "Managed property defined in search configuration XML, is missing \"{0}\" attribute.";
        private const string CategoryAttributeMissingLogFormat =
            "Category (under crawled properties) is missing \"{0}\" attribute.";
        private const string LogCategory = "Search Configuration";
        private const string PropertyRemoveFailedNotFoundLogFormat = "Removing managed property failed. Managed property \"{0}\" was not found.";
        private const string PropertyRemoveFailedNameMissingLog = "Removing managed property failed. \"Name\" attribute is empty or does not exist in <remove /> element.";
        private const string CrawledPropertyRemoveFailedNotFoundLogFormat = "Removing crawled property failed. Managed property \"{0}\" was not found.";
        private const string CrawledPropertyRemoveFailedNameMissingLog = "Removing crawled property failed. \"Name\" attribute missing from";
        private readonly SPServiceContext _context;
        private readonly SPSite _site;

        /// <summary>
        /// When using this constructor, the default search service application is used to create the service context.
        /// </summary>
        public SearchAdminUtility()
        {

            // Get the default service context
            _context = SPServiceContext.GetContext(SPServiceApplicationProxyGroup.Default, SPSiteSubscriptionIdentifier.Default);
        }

        /// <summary>
        /// When using this constructor, the given site collection is used to create the service context.
        /// </summary>
        /// <param name="site">SPSite object to be used when constructing service context</param>
        public SearchAdminUtility(SPSite site)
        {
            // Get the service context related to the given site
            _site = site;
            _context = SPServiceContext.GetContext(site);
        }

        #region Meta-data Properties

        /// <summary>
        /// Based on the configuration XML provided, this method can:
        /// * Clear unmapped crawled properties (making sure that new ones in the XML are cleared).
        /// * Add new crawled properties.
        /// * Delete managed properties.
        /// * Add new managed properties.
        /// * Map managed properties to crawled properties.
        /// </summary>
        /// <param name="config">XML document containing search property configurations.</param>
        public void RebuildMetadataProperties(XDocument config)
        {
            
            // Get the default service context
            //SPServiceContext context = SPServiceContext.GetContext(SPServiceApplicationProxyGroup.Default, SPSiteSubscriptionIdentifier.Default);

            RebuildMetadataProperties(config, _context);
        }

        private static void RebuildMetadataProperties(XNode config, SPServiceContext context)
        {
            // Get the search service application proxy
            var searchProxy = SearchServiceApplicationProxy.GetProxy(context);
            // Another alternative is the following line ;)
            // * var searchProxy =
            //   context.GetDefaultProxy(typeof (SearchServiceApplicationProxy)) as SearchServiceApplicationProxy;

            if (searchProxy == null)
                throw new InvalidOperationException("Search Service Application was not found.");

            // Get the search service application info object so we can find the Id of 
            // our Search Service Application.
            var ssai = searchProxy.GetSearchServiceApplicationInfo();

            // Get the application itself
            var searchApp = SearchService.Service.SearchApplications.GetValue<SearchServiceApplication>(ssai.SearchServiceApplicationId);

            // Get the schema of our Search Service Application
            var schema = new Schema(searchApp);

            var crawledPropsCache = new List<CrawledProperty>();
            var categories = schema.AllCategories;

            // Remove Managed Properties
            var managedPropsRemove = config.XPathSelectElements("/SearchConfiguration/ManagedProperties/remove");
            RemoveManagedProperties(schema, managedPropsRemove);

            var loadedCategories = new HashSet<string>();

            // Add / update crawled properties under different categories
            // SearchConfiguration > CrawledProperties > Category
            foreach (var categoryCfg in config.XPathSelectElements("/SearchConfiguration/CrawledProperties/Category"))
            {
                // If crawled properties in this category are not loaded
                // load them and add category name to the list.
                var categoryName = TryGetAttributeValue(categoryCfg, "Name");
                if (string.IsNullOrEmpty(categoryName))
                {
                    EproFramework.LogMessage(SeverityLevels.Critical, LogCategory,
                        string.Format(CategoryAttributeMissingLogFormat, "Name"));
                    continue;
                }
                var cat = categories[categoryName];
                if (!loadedCategories.Contains(categoryName))
                {
                    crawledPropsCache.AddRange(categories[categoryName].GetAllCrawledProperties().Cast<CrawledProperty>());
                    loadedCategories.Add(categoryName);
                }

                // SearchConfiguration > CrawledProperties > Category > * (clear | CrawledProperty)
                foreach (var crawledPropCfg in categoryCfg.Elements())
                {
                    if (crawledPropCfg.Name == "clear")
                    {
                        ClearCrawledPropertiesInCategory(crawledPropsCache, cat, categoryCfg);
                    }
                    else if (crawledPropCfg.Name == "CrawledProperty")
                    {
                        // Create the crawled property if it doesn't exist
                        CreateCrawledPropertyIfDoesNotExist(crawledPropsCache, cat, crawledPropCfg);
                    }
                }

            }
            // Get all the managed properties
            // Create all required managed properties
            // SearchConfiguration > ManagedProperties > ManagedProperty
            // foreach (var managedPropCfg in config.Element("SearchConfiguration").Element("ManagedProperties").Elements("ManagedProperty"))
            foreach (var managedPropCfg in config.XPathSelectElements("/SearchConfiguration/ManagedProperties/ManagedProperty"))
            {
                var managedPropName = TryGetAttributeValue(managedPropCfg, "Name");
                if (string.IsNullOrEmpty(managedPropName))
                {
                    EproFramework.LogMessage(SeverityLevels.Critical, LogCategory,
                        string.Format(UnknownManagedPropertyAttributeMissingLogFormat, "Name"));
                    continue;
                }
                var managedPropType = TryGetAttributeValue(managedPropCfg, "Type");
                if (string.IsNullOrEmpty(managedPropType))
                {
                    EproFramework.LogMessage(SeverityLevels.Critical, LogCategory,
                        string.Format(KnownManagedPropertyAttributeMissingLogFormat, managedPropName, "Type"));
                    continue;
                }
                var managedProp = CreateOrGetManagedProperty(schema, managedPropName, managedPropType);

                // Create all the required mappings for the current Managed Property
                var isMappingChanged = false;
                MappingCollection mappings = null;
                foreach (var mapCfg in managedPropCfg.Elements())
                {
                    if (mapCfg.Name == "clear")
                    {
                        // Clear all mappings of this ManagedProperty
                        managedProp.DeleteAllMappings();
                        isMappingChanged = true;
                    }
                    else if (mapCfg.Name == "Map")
                    {
                        // Add new mappings
                        mappings = managedProp.GetMappings();
                        var crawledPropName = mapCfg.Value;
                        var mappingCategoryName = TryGetAttributeValue(mapCfg, "Category");
                        var crawledProp = FindCrawledProperty(schema, crawledPropName, mappingCategoryName,
                            crawledPropsCache, loadedCategories);

                        // Map the managed property to the crawled property (if found)
                        if (crawledProp != null)
                        {
                            var mapping = new Mapping(
                                crawledProp.Propset,
                                crawledPropName,
                                crawledProp.VariantType,
                                managedProp.PID);
                            if (!mappings.Contains(mapping))
                            {
                                mappings.Add(mapping);
                                isMappingChanged = true;
                            }
                        }
                        else
                        {
                            EproFramework.LogMessage(SeverityLevels.Critical, LogCategory,
                                string.Format(PropertyMappingFailedExceptionFormat, managedPropName));
                        }
                    }
                }

                if (isMappingChanged) managedProp.SetMappings(mappings);
            }
        }

        private static CrawledProperty FindCrawledProperty(Schema schema, string crawledPropName, string categoryName,
            IEnumerable<CrawledProperty> propertyCache, ICollection<string> categoryNames)
        {
            var crawledPropsMatchCount = 0;
            if (categoryNames.Contains(categoryName))
            {
                return propertyCache.FirstOrDefault(cp => cp.CategoryName == categoryName && cp.Name == crawledPropName);
            }
            // If crawled property was not found in the cache, check the DB!
            var result =
                schema.AllCategories[categoryName].QueryCrawledProperties(crawledPropName, 1,
                    Guid.Empty, string.Empty, false) as List<CrawledProperty>;
            if (result != null && result.Count == 1)
            {
                return result[0];
            }
            if (result != null && result.Count > 1)
            {
                EproFramework.LogMessage(LoggingType.Critical,
                    string.Format(PropertyMappingFailedMutipleMatchLogFormat,
                        crawledPropName,
                        categoryName,
                        crawledPropsMatchCount));
            }
            else
            {
                EproFramework.LogMessage(LoggingType.Critical,
                    string.Format(PropertyMappingFailedNotFoundLogFormat,
                        crawledPropName,
                        categoryName));
            }
            return null;
        }

        private static ManagedProperty CreateOrGetManagedProperty(Schema schema, string managedPropName, string managedPropType)
        {
            ManagedProperty managedProp;
            if (!schema.AllManagedProperties.Contains(managedPropName))
            {
                try
                {
                    managedProp = schema.AllManagedProperties.Create(
                        managedPropName,
                        (ManagedDataType)Enum.Parse(typeof(ManagedDataType),
                            managedPropType));
                }
                catch (Exception ex)
                {
                    EproFramework.LogMessage(LoggingType.Critical,
                        string.Format(CreateManagedPropertyExceptionFormat,
                            managedPropName,
                            ex));
                    throw;
                }
            }
            else
            {
                managedProp = schema.AllManagedProperties[managedPropName];
            }

            return managedProp;
        }

        private static void ClearCrawledPropertiesInCategory(ICollection<CrawledProperty> crawledPropsCache, Category cat, XElement categoryCfg)
        {
            // Remove existing crawled properties from index
            // to make sure they will be deleted
            foreach (var propName in categoryCfg.Elements("CrawledProperty").Select(remove => TryGetAttributeValue(remove, "Name")))
            {
                if (string.IsNullOrEmpty(propName))
                {
                    EproFramework.LogMessage(SeverityLevels.Warning, LogCategory,
                        CrawledPropertyRemoveFailedNameMissingLog);
                    continue;
                }
                var propToRemove = crawledPropsCache.FirstOrDefault(cp => cp.Name == propName);
                if (propToRemove == null)
                {
                    EproFramework.LogMessage(SeverityLevels.Warning, LogCategory,
                        string.Format(CrawledPropertyRemoveFailedNotFoundLogFormat, propName));
                    continue;
                }
                crawledPropsCache.Remove(propToRemove);
                propToRemove.IsMappedToContents = false;
                propToRemove.Update();
            }
            // Delete unmapped properties
            cat.DeleteUnmappedProperties();
        }

        private static void CreateCrawledPropertyIfDoesNotExist(ICollection<CrawledProperty> crawledPropsCache, Category cat, XElement crawledPropCfg)
        {
            var crawledProp = crawledPropsCache.FirstOrDefault(cp => cp.Name == TryGetAttributeValue(crawledPropCfg, "Name"));
            if (crawledProp == null)
            {
                try
                {
                    crawledProp = cat.CreateCrawledProperty(TryGetAttributeValue(crawledPropCfg, "Name"),
                        false,
                        new Guid(TryGetAttributeValue(crawledPropCfg, "PropSetId")),
                        int.Parse(TryGetAttributeValue(crawledPropCfg, "VariantType")));
                    crawledPropsCache.Add(crawledProp);
                }
                catch (Exception ex)
                {
                    EproFramework.LogMessage(LoggingType.Critical,
                        string.Format(CreateCrawledPropertyExceptionFormat,
                            TryGetAttributeValue(crawledPropCfg, "Name"),
                            ex));
                    throw;
                }
            }
        }

        private static void RemoveManagedProperties(Schema schema, IEnumerable<XElement> managedPropsRemove)
        {
            foreach (var propName in managedPropsRemove.Select(remove => TryGetAttributeValue(remove, "Name")))
            {
                if (string.IsNullOrEmpty(propName))
                {
                    EproFramework.LogMessage(SeverityLevels.Warning, LogCategory,
                        PropertyRemoveFailedNameMissingLog);
                    continue;
                }
                if (!schema.AllManagedProperties.Contains(propName))
                {
                    EproFramework.LogMessage(SeverityLevels.Warning, LogCategory,
                        string.Format(PropertyRemoveFailedNotFoundLogFormat, propName));
                    continue;
                }
                var managedProp = schema.AllManagedProperties[propName];
                managedProp.DeleteAllMappings();
                managedProp.Delete();
            }
        }

        #endregion

        #region Content Sources

        public void RecreateContentSources(XDocument config)
        {

            // Get the default service context
            //SPServiceContext context = SPServiceContext.GetContext(SPServiceApplicationProxyGroup.Default, SPSiteSubscriptionIdentifier.Default);

            RecreateContentSources(config, _context);
        }

        private void RecreateContentSources(XNode config, SPServiceContext context)
        {
            // Get the search service application proxy
            var searchProxy = SearchServiceApplicationProxy.GetProxy(context);

            if (searchProxy == null)
                throw new Exception(DefaultSsaProxyNotFoundExceptionMessage);

            var ssaInfo = searchProxy.GetSearchServiceApplicationInfo();
            var searchApp = SearchService.Service.SearchApplications.GetValue<SearchServiceApplication>(ssaInfo.SearchServiceApplicationId);
            var content = new Content(searchApp);

            // Remove content sources specified by remove tag in configuration XML.
            var removeContentSourceElements = config.XPathSelectElements("/SearchConfiguration/ContentSources/remove");
            foreach (var removeContentSourceElement in removeContentSourceElements)
            {
                var contentSourceName = TryGetAttributeValue(removeContentSourceElement, "Name");
                if (content.ContentSources.Exists(contentSourceName))
                {
                    var contentSource = content.ContentSources[contentSourceName];
                    contentSource.Delete();
                }
                else
                {
                    EproFramework.LogMessage(
                        SeverityLevels.Warning,
                        LogCategory,
                        string.Format(ContentSourceNotFoundRemovalFailedLogFormat,
                            contentSourceName));
                }
            }

            // Create new Content Sources (if they don't exist)
            var contentSourceElements = config.XPathSelectElements("/SearchConfiguration/ContentSources/ContentSource");
            foreach (var contentSourceElement in contentSourceElements)
            {
                var contentSourceName = TryGetAttributeValue(contentSourceElement,"Name");
                var contentSourceExists = content.ContentSources.Exists(contentSourceName);
                if (contentSourceExists)
                {
                    EproFramework.LogMessage(
                        SeverityLevels.Information,
                        LogCategory,
                        string.Format(ContentSourceExistsLogFormat, 
                            contentSourceName));
                    var recreateAttr = contentSourceElement.Attribute("RecreateIfExists");
                    if (recreateAttr != null && bool.Parse(recreateAttr.Value))
                    {
                        var contentSource = content.ContentSources[contentSourceName];
                        if (contentSource.StartAddresses.Count > 0)
                        {
                            contentSource.StartAddresses.Clear();
                            contentSource.Update();
                        }
                        contentSource.Delete();
                        contentSourceExists = false;
                    }
                }
                if (!contentSourceExists)
                {
                    var contentSourceTypeName = TryGetAttributeValue(contentSourceElement,"Type");
                    var startFullCrawl = bool.Parse(contentSourceElement.AttributeOrDefault("StartFullCrawl", "false"));
                    var contentSourceType = GetContentSourceTypeFromString(contentSourceTypeName);
                    var contentSource = content.ContentSources.Create(contentSourceType, contentSourceName);
                    ConstructStartAddresses(contentSource, contentSourceElement.Elements("StartAddress"));
                    contentSource.Update();
                    if (startFullCrawl)
                        contentSource.StartFullCrawl();                    
                }
            }
        }

        private static Type GetContentSourceTypeFromString(string contentSourceTypeName)
        {
            switch (contentSourceTypeName)
            {
                case "BusinessData":
                    return typeof(BusinessDataContentSource);
                case "Web":
                    return typeof(WebContentSource);
                default:
                    throw new InvalidOperationException(
                        string.Format(ContentSourceNotSupportedExceptionFormat,
                            contentSourceTypeName));
            }
        }

        /// <summary>
        /// Constructs a url start address for a given application name. If appName is empty the start address returned is one that will crawl all applications on the ssp
        /// </summary>
        /// <remarks>
        /// If {sitecollection} is used in the StartAddress's Url attribute, a SPSite must have been passed to the 
        /// constructor of the class.
        /// </remarks>
        private static Uri ConstructBusinessDataStartAddress(XElement definition)
        {
            // Extract values from attributes
            var lobSystemName = definition.Attribute("LobSystemName").ValueOrDefault(string.Empty);
            var lobSystemInstanceName = definition.Attribute("LobSystemInstanceName").ValueOrDefault(string.Empty);
            var bdcAppProxyGroupName = definition.Attribute("BdcAppProxyGroupName").ValueOrDefault("Default");
            var partitionId = new Guid(definition.Attribute("PartitionId").ValueOrDefault(Guid.Empty.ToString()));

            // Construct the start address
            return BusinessDataContentSource.ConstructStartAddress(bdcAppProxyGroupName, partitionId, lobSystemName, lobSystemInstanceName);
        }

        private Uri ConstructWebStartAddress(XElement definition)
        {
            var startUrl = TryGetAttributeValue(definition, "Url");
            if (startUrl.Contains("{sitecollection}")) startUrl = startUrl.Replace("{sitecollection}", _site.Url);
            return new Uri(startUrl);
        }

        private void ConstructStartAddresses(ContentSource contentSource, 
            IEnumerable<XElement> startAddressElements)
        {
            Func<XElement, Uri> startAddressConstructor;
            if (contentSource.Type == ContentSourceType.Web)
                startAddressConstructor = ConstructWebStartAddress;
            else if (contentSource.Type == ContentSourceType.Business)
                startAddressConstructor = ConstructBusinessDataStartAddress;
            else
                throw new InvalidOperationException(
                    string.Format(ContentSourceNotSupportedExceptionFormat,
                        contentSource.Type));
            foreach (var startAddressElement in startAddressElements)
            {
                contentSource.StartAddresses.Add(startAddressConstructor(startAddressElement));
            }
        }

        #endregion

        private static string TryGetAttributeValue(XElement element, string attributeName)
        {
            var attribute = element.Attribute(attributeName);
            return attribute == null ? null : attribute.Value;
        }
    }
}
