const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const logger = require('./lib/logger');

const { filterNullValuesFromObject, goal } = require('./utils');

const Domain = require('./models/Domain');

const CRON_CONFIG = {
  cronName: 'DP',
  processRetryCount: 4,
  processBatchLimit: 100,
  processMaxIterationPageCount: 100,
  queueBatchSize: 2000,
};

const {
  cronName,
  processRetryCount,
  processBatchLimit, 
  processMaxIterationPageCount,
  queueBatchSize,
} = CRON_CONFIG;
const LOG_PREFIX = `[CRON][Daily][${cronName}]`;


const hubspotClient = new hubspot.Client();

let expirationDate;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async (domain) => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = processBatchLimit;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount < processRetryCount) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error(`Failed to fetch companies for the ${processRetryCount}th time. Aborting.`);

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    logger.info(
      `${LOG_PREFIX}[Companies]: Batch - ${
        offsetObject?.after - processBatchLimit > 0
          ? `${offsetObject?.after - processBatchLimit} -`
          : !offsetObject?.after
          ? "Last"
          : "0 -"
      } ${offsetObject?.after || ""}`
    );

    data.forEach(company => {
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
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= (processMaxIterationPageCount - 1) * processBatchLimit) {
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = processBatchLimit;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount < processRetryCount) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error(`Failed to fetch contacts for the ${processRetryCount}th time. Aborting.`);

    const data = searchResult.results || [];

    logger.info(
      `${LOG_PREFIX}[Contacts]: Batch - ${
        offsetObject?.after - processBatchLimit > 0
          ? `${offsetObject?.after - processBatchLimit} -`
          : !offsetObject?.after
          ? "Last"
          : "0 -"
      } ${offsetObject?.after || ""}`
    );

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= (processMaxIterationPageCount - 1) * processBatchLimit) {
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified meetings as 100 meetings per page
 */
const processMeetings = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.deals);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = processBatchLimit;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'dealname',
        'amount',
        'pipeline',
        'dealstage',
        'hs_lastmodifieddate',
        'hs_createdate',
        'createdate',
        'closedate',
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount < processRetryCount) {
      try {
        searchResult = await hubspotClient.crm.deals.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error(`Failed to fetch meetings for the ${processRetryCount}th time. Aborting.`);

    const data = searchResult.results || [];

    logger.info(
      `${LOG_PREFIX}[Meetings]: Batch - ${
        offsetObject?.after - processBatchLimit > 0
          ? `${offsetObject?.after - processBatchLimit} -`
          : !offsetObject?.after
          ? "Last"
          : "0 -"
      } ${offsetObject?.after || ""}`
    );

    // 1. Collect meeting IDs
    const meetingIds = data.map(meeting => meeting.id);

    // 2. Fetch associations: deals (meetings) to contacts
    let meetingContactAssociations = {};
    if (meetingIds.length > 0) {
      const associationsResults = (await (await hubspotClient.apiRequest({
        method: 'post',
        path: '/crm/v3/associations/DEALS/CONTACTS/batch/read',
        body: { inputs: meetingIds.map(id => ({ id })) }
      })).json())?.results || [];
      // Map: meetingId -> [contactId, ...]
      meetingContactAssociations = Object.fromEntries(
        associationsResults.map(a => [a.from.id, (a.to || []).map(t => t.id)])
      );
    }

    // 3. Collect all unique contact IDs
    const allContactIds = Array.from(new Set(Object.values(meetingContactAssociations).flat()));
    let contactIdToEmail = {};
    if (allContactIds.length > 0) {
      // HubSpot batch read for contacts (max 100 per call)
      for (let i = 0; i < allContactIds.length; i += 100) {
        const batch = allContactIds.slice(i, i + 100);
        const contactsResult = (await (await hubspotClient.apiRequest({
          method: 'post',
          path: '/crm/v3/objects/contacts/batch/read',
          body: { properties: ['email'], inputs: batch.map(id => ({ id })) }
        })).json())?.results || [];
        contactsResult.forEach(contact => {
          contactIdToEmail[contact.id] = contact.properties?.email || null;
        });
      }
    }

    // 4. For each meeting, create an action for each associated contact (with email)
    data.forEach(meeting => {
      if (!meeting.properties) return;
      const isCreated = new Date(meeting.createdAt) > lastPulledDate;
      const meetingProps = {
        meeting_id: meeting.id,
        dealname: meeting.properties.dealname,
        amount: meeting.properties.amount,
        pipeline: meeting.properties.pipeline,
        dealstage: meeting.properties.dealstage,
        createdate: meeting.properties.createdate,
        closedate: meeting.properties.closedate,
        hs_lastmodifieddate: meeting.properties.hs_lastmodifieddate,
        hs_createdate: meeting.properties.hs_createdate,
      };
      const contactIds = meetingContactAssociations[meeting.id] || [];
      if (contactIds.length === 0) {
        // If no contact, still push the meeting action (with no email)
        q.push({
          actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
          actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
          includeInAnalytics: 0,
          meetingProperties: meetingProps,
          contact_email: null
        });
      } else {
        contactIds.forEach(contactId => {
          const email = contactIdToEmail[contactId] || null;
          q.push({
            actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
            actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
            includeInAnalytics: 0,
            meetingProperties: meetingProps,
            contact_email: email
          });
        });
      }
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= (processMaxIterationPageCount - 1) * processBatchLimit) {
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }

    account.lastPulledDates.deals = now;
    await saveDomain(domain);
    console.log('fetch meeting batch');
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
      console.error('goal is failed:', {
        errorMessage: err?.message,
        errorStack: err?.stack,
      });
    }
  }

  callback();
}, 5);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    try {
      await goal(actions);
    } catch (err) {
      // TODO: DLQ or other implementation required for fail scenario
      console.error('drainQueue - goal is failed:', {
        errorMessage: err?.message,
        errorStack: err?.stack,
      });
    }
  }

  return true;
};

const pullDataFromHubspot = async () => {
  logger.info(`${LOG_PREFIX}[Start]: HubSpot`);

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    logger.info(`${LOG_PREFIX}[Account][Start]: HubSpot - ${account.hubId}`);

    try {
      await refreshAccessToken(domain, account.hubId);

      logger.info(`${LOG_PREFIX}: Access Token Refreshed`);
    } catch (err) {
      logger.error({ 
        apiKey: domain.apiKey, 
        metadata: { operation: 'refreshAccessToken' },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Error]: refreshAccessToken`);
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);

      logger.info(`${LOG_PREFIX}[Contacts]: Processed`);
    } catch (err) {
      logger.error({ 
        apiKey: domain.apiKey, 
        metadata: { 
          operation: 'processContacts',
          hubId: account.hubId,
        },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Error]: processContacts`);
    }

    try {
      await processCompanies(domain, account.hubId, q);
      
      logger.info(`${LOG_PREFIX}[Companies]: Processed`);
    } catch (err) {
      logger.error({ 
        apiKey: domain.apiKey, 
        metadata: { 
          operation: 'processCompanies',
          hubId: account.hubId,
        },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Error]: processCompanies`);
    }

    try {
      await processMeetings(domain, account.hubId, q);
      
      logger.info(`${LOG_PREFIX}[Meetings]: Processed`);
    } catch (err) {
      logger.error({ 
        apiKey: domain.apiKey, 
        metadata: { 
          operation: 'processMeetings',
          hubId: account.hubId,
        },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Error]: processMeetings`);
    }

    try {
      await drainQueue(domain, actions, q);
      
      logger.info(`${LOG_PREFIX}[Queue]: Drained`);
    } catch (err) {
      logger.error({ 
        apiKey: domain.apiKey, 
        metadata: { 
          operation: 'drainQueue',
          hubId: account.hubId,
        },
        errorMessage: err?.message,
        errorStack: err?.stack, 
      }, `${LOG_PREFIX}[Error][Queue]: drainQueue Failed`);
    }

    await saveDomain(domain);

    logger.info(`${LOG_PREFIX}[Account][Start]: HubSpot - ${account.hubId}`);
  }
  
  logger.info(`${LOG_PREFIX}[Finish]: HubSpot`);

  process.exit();
};

module.exports = pullDataFromHubspot;
