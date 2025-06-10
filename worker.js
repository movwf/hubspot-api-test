import _ from "lodash";
import { queue } from "async";

import getHubspotClient from "./clients/hubspot";

import retry from "./lib/retry";
import logger from "./lib/logger";
import { SearchQuery } from "./lib/hubspot";
import { filterNullValuesFromObject, goal } from "./utils";

import Domain from "./models/Domain";

const { HUBSPOT_CID, HUBSPOT_CS } = process.env;

const CRON_CONFIG = {
  cronName: "DP",
  processRetryCount: 4,
  processRetryDelay: 1500,
  processBatchLimit: 100,
  processMaxIterationPageCount: 100,
  queueBatchSize: 2000,
  queueConcurrency: 5,
};

const {
  cronName,
  processRetryCount,
  processRetryDelay,
  processBatchLimit,
  processMaxIterationPageCount,
  queueBatchSize,
  queueConcurrency,
} = CRON_CONFIG;
const LOG_PREFIX = `[CRON][Daily][${cronName}]`;

const _LAST_MODIFIED_FILTER_PROP_NAME_BY_OBJECT_NAME = {
  contacts: "lastmodifieddate",
  companies: "hs_lastmodifieddate",
  meetings: "hs_lastmodifieddate",
};

const _SEARCH_SETTINGS_BY_OBJECT_NAME = {
  contacts: {
    properties: [
      "firstname",
      "lastname",
      "jobtitle",
      "email",
      "hubspotscore",
      "hs_lead_status",
      "hs_analytics_source",
      "hs_latest_source",
    ],
    sort: [
      {
        propertyName: _LAST_MODIFIED_FILTER_PROP_NAME_BY_OBJECT_NAME.contacts,
        direction: "ASCENDING",
      },
    ],
    associations: { from: "contacts", to: "companies" }
  },
  companies: {
    properties: [
      "name",
      "domain",
      "country",
      "industry",
      "description",
      "annualrevenue",
      "numberofemployees",
      "hs_lead_status",
    ],
    sort: [
      {
        propertyName: _LAST_MODIFIED_FILTER_PROP_NAME_BY_OBJECT_NAME.companies,
        direction: "ASCENDING",
      },
    ],
  },
  meetings: {
    properties: ["hs_createdate", "hs_lastmodifieddate", "hs_object_id"],
    sort: [
      {
        propertyName: _LAST_MODIFIED_FILTER_PROP_NAME_BY_OBJECT_NAME.meetings,
        direction: "ASCENDING",
      },
    ],
    associations: { from: "meetings", to: "contacts" },
  },
};

const generateLastModifiedDateFilter = (date, nowDate, objectName) => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        {
          propertyName:
              _LAST_MODIFIED_FILTER_PROP_NAME_BY_OBJECT_NAME[objectName],
          operator: "GTE",
          value: `${date.valueOf()}`,
        },
        {
          propertyName:
              _LAST_MODIFIED_FILTER_PROP_NAME_BY_OBJECT_NAME[objectName],
          operator: "LTE",
          value: `${nowDate.valueOf()}`,
        },
      ],
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  // eslint-disable-next-line no-unreachable
  domain.markModified("integrations.hubspot.accounts");
  await domain.save();
};

/**
 * Get access token from HubSpot
 * @param {import('@hubspot/api-client').Client} hsClient
 * @param {import("./types").HubspotAccount} account
 */
const refreshAccessToken = async (hsClient, account) => {
  const { accessToken, refreshToken } = account;

  return hsClient.oauth.tokensApi
    .create(
      "refresh_token",
      undefined,
      undefined,
      HUBSPOT_CID,
      HUBSPOT_CS,
      refreshToken
    )
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      account.expirationDate = new Date(
        body.expiresIn * 1000 + new Date().getTime()
      );

      hsClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * @typedef {{
 *  associations: Record<string, string>,
 *  targets: Record<string, any>,
 * }} AssocContext
 */

/**
 * Processes contacts and creates corresponding action
 * and pushes to queue
 * @param {import("async").QueueObject} q 
 * @param {*} contact 
 * @param {AssocContext} assocCtx 
 * @param {string} lastPulledDate 
 * @returns 
 */
const processContacts = (q, contact, assocCtx, lastPulledDate) => {
  if (!contact.properties || !contact.properties.email) return;

  const companyId = assocCtx.associations[contact.id];

  
  const userProperties = {
    company_id: companyId,
    contact_name: (
      (contact.properties.firstname || "") +
      " " +
      (contact.properties.lastname || "")
    ).trim(),
    contact_title: contact.properties.jobtitle,
    contact_source: contact.properties.hs_analytics_source,
    contact_status: contact.properties.hs_lead_status,
    contact_score: parseInt(contact.properties.hubspotscore) || 0,
  };
  
  const actionTemplate = {
    includeInAnalytics: 0,
    identity: contact.properties.email,
    userProperties: filterNullValuesFromObject(userProperties),
  };
  
  const isCreated = new Date(contact.createdAt) > lastPulledDate;
  q.push({
    actionName: isCreated ? "Contact Created" : "Contact Updated",
    actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
    ...actionTemplate,
  });
};

/**
 * Processes companies and creates corresponding action
 * and pushes to queue
 * @param {import("async").QueueObject} q 
 * @param {*} contact 
 * @param {AssocContext} assocCtx 
 * @param {string} lastPulledDate 
 * @returns 
 */
const processCompanies = (q, company, assocCtx, lastPulledDate) => {
  if (!company.properties) return;

  const actionTemplate = {
    includeInAnalytics: 0,
    companyProperties: {
      company_id: company.id,
      company_domain: company.properties.domain,
      company_industry: company.properties.industry
    }
  };

  
  const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);
  q.push({
    actionName: isCreated ? 'Company Created' : 'Company Updated',
    actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
    ...actionTemplate
  });
};

/**
 * Processes meetings and creates corresponding action
 * and pushes to queue
 * @param {import("async").QueueObject} q 
 * @param {*} contact 
 * @param {AssocContext} assocCtx 
 * @param {string} lastPulledDate 
 * @returns 
 */
const processMeetings = (q, meeting, assocCtx, lastPulledDate) => {
  if (!meeting.properties) return;
  
  const groupAttendeeEntries = _.groupBy(
    Object.entries(assocCtx.associations),
    ([key]) => key
  );
  
  const attendeeIds = _.mapValues(groupAttendeeEntries, entries => entries.map(([,value]) => value));
  
  const attendeeList = attendeeIds[meeting.id]?.length ?
    attendeeIds[meeting.id].map(id => ({ email: assocCtx.targets[id].email })):
    [];
  
  const actionTemplate = {
    includeInAnalytics: 0,
    meetingProperties: {
      meeting_id: meeting.id,
      meeting_name: meeting.properties.name,
      attendees: attendeeList,
    }
  };

  const isCreated = new Date(meeting.createdAt) > lastPulledDate;
  q.push({
    actionName: isCreated ? "Meeting Created" : "Meeting Updated",
    actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
    ...actionTemplate,
  });
};

/**
 * Scan single object with/wo associations to process
 * @param {import("./types").HubspotObjectName} objectName
 * @param {import('./types').HubspotAccount} account
 * @param {import("async").QueueObject} q
 * @returns
 */
const scanObject = async (objectName, account, q) => {
  const lastPulledDate = new Date(
    account.lastPulledDates[objectName] ||
      account._doc.lastPulledDates[objectName] ||
      null
  );
  const now = new Date();

  const offsetObject = {
    lastModifiedDate: lastPulledDate,
    after: 0,
  };

  const hsClient = getHubspotClient({ accessToken: "" });
  const searchQ = new SearchQuery(objectName, hsClient);

  searchQ.selectProperties(
    _SEARCH_SETTINGS_BY_OBJECT_NAME[objectName].properties
  );
  searchQ.addSort(_SEARCH_SETTINGS_BY_OBJECT_NAME[objectName].sort);
  searchQ.setLimit(processBatchLimit);

  let hasMore = true;
  let page = 1;

  while (hasMore) {
    const lastModifiedDateFilter = generateLastModifiedDateFilter(
      offsetObject.lastModifiedDate,
      now,
      objectName
    );

    let searchResult = {};

    //#region Initial Data Fetch from Hubspot
    try {
      if (new Date() > (account.expirationDate || 0)) {
        await refreshAccessToken(hsClient, account);
      }

      searchResult = await retry(
        () =>
          searchQ
            .replaceFilterGroups([lastModifiedDateFilter])
            .paginate(offsetObject.after)
            .exec(),
        processRetryCount,
        processRetryDelay
      );
    } catch (error) {
      logger.error(error);

      throw new Error(
        `Failed to fetch ${objectName} for the ${processRetryCount}th time. Aborting.`
      );
    }
    //#endregion

    const searchData = searchResult.results;
    offsetObject.after = parseInt(searchResult.paging?.next?.after);

    logger.info(
      `${LOG_PREFIX}[${objectName}][${page}]: Batch - ${
        offsetObject?.after - processBatchLimit > 0 ?
          `${offsetObject?.after - processBatchLimit} -` :
          !offsetObject?.after ?
            "Last" :
            "0 -"
      } ${offsetObject?.after || ""}`
    );

    //#region Associations
    const assocConfig = _SEARCH_SETTINGS_BY_OBJECT_NAME[objectName]?.associations;

    const assocCtx = {
      associations: {},
      targets: {},
    };

    if (assocConfig) {
      const objectIds = searchData.map(object => object.id);

      const associationsResults = await hsClient.crm.associations.v4.batchApi.getPage(
        assocConfig.from,
        assocConfig.to,
        { inputs: objectIds.map(id => ({ id })) }
      );

      assocCtx.associations = associationsResults.results.reduce((prev, assoc) => ({
        ...prev,
        ...assoc._from && { [assoc._from.id]: assoc.to[0].toObjectId },
      }), {});

      const targetIds = new Set(Object.values(assocCtx.associations));

      const targetResults = await hsClient.crm[assocConfig.to].batchApi.read(
        { inputs: [...targetIds.values()].map(id => ({ id })) }
      );

      assocCtx.targets = targetResults.results.reduce((prev, target) => ({
        ...prev,
        [target.id]: target.properties,
      }), {});
    }
    //#endregion

    //#region Data Processing
    for (const object of searchData) {
      (({
        contacts: processContacts,
        companies: processCompanies,
        meetings: processMeetings,
      })[objectName])(q, object, assocCtx, lastPulledDate);
    }
    //#endregion

    //#region Pagination
    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (
      offsetObject?.after >=
      (processMaxIterationPageCount - 1) * processBatchLimit
    ) {
      offsetObject.lastModifiedDate = new Date(
        searchData[searchData.length - 1].updatedAt
      ).valueOf();
    }

    page += 1;
    //#endregion
  }

  account.lastPulledDates[objectName] = now;
  // TODO: Pass domain
  await saveDomain();

  return true;
};

/**
 * Process given objects
 * @param {import("./types").HubspotObjectName} objectsList
 * @param {string} hubId
 * @param {import('./types').HubspotAccount} account
 * @param {import("async").QueueObject} q
 */
const processObjects = async (objectsList, account, q) =>
  Promise.all(
    objectsList.map(objectName => scanObject(objectName, account, q))
  );

const drainQueue = async (actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    try {
      await goal(actions);
    } catch (err) {
      // TODO: DLQ or other implementation required for fail scenario
      console.error(`${LOG_PREFIX}[Drain-Queue][DB][Save][Error]: Faild to save to db`, {
        errorMessage: err?.message,
        errorStack: err?.stack,
        count: actions.length,
      });
    }
  }

  return true;
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > queueBatchSize) {
    logger.info({
      apiKey: domain.apiKey,
      count: actions.length,
    }, `${LOG_PREFIX}[Queue]: Inserting Actions to DB`);

    const actionsToSave = actions.splice(0, actions.length);

    try {
      await goal(actionsToSave);
    } catch (err) {
      // TODO: DLQ or other implementation required for fail scenario
      logger.error(`${LOG_PREFIX}[Queue][DB][Save][Error]: Faild to save to db`, {
        errorMessage: err?.message,
        errorStack: err?.stack,
        count: actionsToSave.length,
      });
    }
  }

  callback();
}, queueConcurrency);

const pullDataFromHubspot = async () => {
  const domain = await Domain.findOne();

  for await (const account of domain.integrations.hubspot.accounts) {
    const actions = [];
    const q = createQueue(domain, actions);

    try {
      logger.info(`${LOG_PREFIX}[Process]: Started`);

      await processObjects([
        "contacts", 
        "companies", 
        "meetings"
      ], account, q);
      
      logger.info(`${LOG_PREFIX}[Process]: Finished`);
    } catch (err) {
      logger.error({
        metadata: { 
          operation: 'processObjects',
          hubId: account.hubId,
        },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Process][Error]: Failed`);
    }

    try {
      logger.info(`${LOG_PREFIX}[Drain-Queue]: Started`);

      await drainQueue(actions, q);
      
      logger.info(`${LOG_PREFIX}[Drain-Queue]: Finished`);
    } catch (err) {
      logger.error({
        metadata: { 
          operation: 'drainQueue',
          hubId: account.hubId,
        },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Drain-Queue][Error]: Failed`);
    }
  }

  process.exit(1);
};

export default pullDataFromHubspot;
