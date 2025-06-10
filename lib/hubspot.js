export class SearchQuery {
  query = {
    filterGroups: [],
    properties: [],
    sort: [],
    limit: 100,
    after: 0,
  };

  /**
   *
   * @param {import('../types').HubspotObjectName} objectName
   * @param {import('@hubspot/api-client').Client} client
   */
  constructor(objectName, client) {
    if (!objectName || !client) {
      throw new Error("SearchQuery: Object name or client is missing.");
    }

    this.objectName = objectName;
    this.client = client;
  }

  setLimit(limit) {
    this.query.limit = limit;

    return this;
  }

  selectProperties(properties) {
    this.query.properties.push(...properties);

    return this;
  }

  addFilterGroups(filterGroups) {
    this.query.filterGroups.push(...filterGroups);

    return this;
  }

  replaceFilterGroups(filterGroups) {
    this.query.filterGroups = filterGroups;

    return this;
  }

  addSort(sort) {
    this.query.sort.push(...sort);

    return this;
  }

  paginate(after) {
    this.query.after = after;

    return this;
  }

  async exec() {
    switch (this.objectName) {
      case 'contacts':
        return this.client.crm.contacts.searchApi.doSearch(this.query);
      case 'companies':
        return this.client.crm.companies.searchApi.doSearch(this.query);
      case 'meetings':
        return this.client.crm.objects.meetings.basicApi.getPage(this.query.limit, 0);
        // NOTE: This API is broken always returns the same page with only 10 results
        // eslint-disable-next-line no-unreachable
        return this.client.crm.objects.meetings.searchApi.doSearch('objectType', this.query);
    }
  }
}
