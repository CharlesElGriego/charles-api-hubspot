const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
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

const saveDomain = async domain => {
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
  const limit = 100;

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
    while (tryCount <= CONFIG.RETRY_LIMIT) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

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
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
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
  const limit = 100;

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
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    console.log('fetch contact batch');

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
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

   //#region  New Code
    try {
      await processAllMeetings(account, domain, processMeetingsBatchV3);
    } catch (err) {
      console.error('Error processing all meetings:', {
        message: err.message,
        metadata: { operation: 'processAllMeetings', hubId: account.hubId },
      });
    }
    //#endregion
    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

//#region New Code - Meeting Processor, HubSpot API
const moment = require('moment');
const CONFIG = {
  MEETINGS_FETCH_LIMIT: 50,
  RETRY_LIMIT: 4,
  TOLERANCE_MS: 1000,
};


/**
 * Processes all meetings with pagination and error handling.
 * @param {Object} account - HubSpot account details.
 * @param {Object} domain - Domain object for logging.
 * @param {Function} processMeetingsBatchV3 - Function to process a batch of meetings.
 * @returns {Promise<void>}
 */
const processAllMeetings = async (account, domain, processMeetingsBatchV3) => {
  const lastPulledDate = new Date(
    account.lastPulledDates.meetings ||
      moment().subtract(4, 'year').toISOString()
  );
  const now = new Date();
  let hasMore = true;
  let offsetAfter = null;
  const limit = CONFIG.MEETINGS_FETCH_LIMIT;

  console.log('\n\n*** Start processing meetings');

  while (hasMore) {
    try {
      // Fetch meetings batch
      const searchResult = await fetchMeetingsBatch(
        lastPulledDate,
        now,
        offsetAfter,
        limit
      );

      const meetings = searchResult?.results || [];
      console.log(`Fetched ${meetings.length} meetings`);

      if (meetings.length > 0) {
        const processedMeetings = await processMeetingsBatchV3(meetings);
        console.log('Processed meetings', processedMeetings);
      }

      // Pagination logic
      offsetAfter = searchResult?.paging?.next?.after || null;
      hasMore = !!offsetAfter;
    } catch (err) {
      console.error('Error fetching or processing meetings batch:', {
        message: err.message,
        offsetAfter,
        metadata: { operation: 'processMeetingsBatch', hubId: account.hubId },
      });
      break; // Stop processing on error
    }
  }

  console.log('Finished processing meetings');
};

/**
 * Fetches a batch of meetings from HubSpot.
 * @param {Date} lastPulledDate - The last pulled date for meetings in ISO 8601 format.
 * @param {Date} now - The current date and time.
 * @param {number} [offsetAfter=0] - Pagination offset to continue fetching meetings.
 * @param {number} [limit=50] - Number of meetings to fetch in a batch (default 50).
 * @returns {Promise<{ results: Object[], paging?: Object }>}
 *          A promise resolving to an object containing meeting results and pagination data.
 */
const fetchMeetingsBatch = async (
  lastPulledDate,
  now,
  offsetAfter = 0,
  limit = 50
) => {
  if (!(lastPulledDate instanceof Date) || !(now instanceof Date)) {
    throw new Error(
      'Invalid date parameters. Ensure lastPulledDate and now are Date objects.'
    );
  }

  const lastModifiedDateFilter = {
    filters: [
      {
        propertyName: 'hs_lastmodifieddate',
        operator: 'GTE',
        value: lastPulledDate.toISOString(), // Standard ISO 8601 format
      },
      {
        propertyName: 'hs_lastmodifieddate',
        operator: 'LTE',
        value: now.toISOString(),
      },
    ],
  };

  const searchObject = {
    filterGroups: [lastModifiedDateFilter],
    sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
    properties: [
      'hs_meeting_title',
      'hs_meeting_start_time',
      'hs_meeting_end_time',
      'hs_meeting_outcome',
      'hs_createdate',
      'hs_lastmodifieddate',
    ],
    limit: Math.min(limit, 100), // HubSpot API max limit is 100
    after: offsetAfter,
  };

  try {
    console.time('fetchMeetings');
    const searchResult =
      await hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
    console.timeEnd('fetchMeetings');

    console.log(
      `Fetched ${searchResult.results.length} meetings, Offset: ${offsetAfter}`
    );
    return searchResult;
  } catch (err) {
    console.error('Error fetching meetings batch:', {
      message: err.message,
      offsetAfter,
      limit,
    });
    throw err; // Re-throw the error for further handling
  }
};

/**
 * Processes a batch of meetings, fetches contact associations and emails, and enriches meeting data.
 * @param {Object[]} meetings - Array of meeting objects from HubSpot API.
 * @returns {Promise<Object[]>} A promise resolving to an array of enriched meetings.
 */
const processMeetingsBatchV3 = async (meetings) => {
  // Extract meeting IDs
  const meetingIds = meetings.map((meeting) => meeting.id);

  // Step 1: Fetch contact associations for meetings
  const contactAssociations = await fetchContactAssociationsBatchV3(meetingIds);

  // Step 2: Fetch unique contact emails for associated contact IDs
  const contactIds = [...new Set(Object.values(contactAssociations).filter(Boolean))]; 
  const contactEmails = await fetchContactEmailsBatchV3(contactIds);

  // Step 3: Enrich meetings with contact associations and other details
  return meetings.map((meeting) =>
    processSingleMeeting(meeting, contactAssociations, contactEmails)
  );
};

/**
 * Enriches a single meeting object with contact email and action metadata.
 * @param {Object} meeting - The raw meeting object.
 * @param {Object} contactAssociations - Map of meeting IDs to contact IDs.
 * @param {Object} contactEmails - Map of contact IDs to their associated emails.
 * @returns {Object} Enriched meeting object with additional metadata.
 */
const processSingleMeeting = (meeting, contactAssociations, contactEmails) => {
  const contactId = contactAssociations[meeting.id];
  const associatedContactEmail = contactEmails[contactId] || null;

  const isCreated =
    Math.abs(new Date(meeting.updatedAt) - new Date(meeting.createdAt)) <=
    CONFIG.TOLERANCE_MS;

  return {
    id: meeting.id,
    actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
    associated_contact_email: associatedContactEmail,
    meeting_title: meeting.properties.hs_meeting_title || 'Unknown Title',
    meeting_start_time: meeting.properties.hs_meeting_start_time || null,
    meeting_end_time: meeting.properties.hs_meeting_end_time || null,
    meeting_outcome: meeting.properties.hs_meeting_outcome || 'No Outcome',
    created_date: meeting.properties.hs_createdate || null,
    last_modified_date: meeting.properties.hs_lastmodifieddate || null,
    archived: meeting.archived || false,
  };
};

/**
 * Fetches contact associations for a batch of meetings from HubSpot.
 * Ensures all meeting IDs are returned, even if no associations exist.
 * @param {string[]} meetingIds - Array of meeting IDs.
 * @returns {Promise<Object>} A map of meeting IDs to associated contact IDs or null.
 */
const fetchContactAssociationsBatchV3 = async (meetingIds) => {
  try {
    const response = await hubspotClient.apiRequest({
      method: 'POST',
      path: '/crm/v3/associations/meetings/contacts/batch/read',
      body: {
        inputs: meetingIds.map((id) => ({ id })),
      },
    });

    const jsonBody = await response.json();

    // Initialize all meetingIds with null
    const associationsMap = Object.fromEntries(
      meetingIds.map((id) => [id, null])
    );

    // Populate associations where available
    jsonBody?.results?.forEach((item) => {
      if (item.from?.id && item.to?.[0]?.id) {
        associationsMap[item.from.id] = item.to[0].id;
      }
    });

    return associationsMap;
  } catch (error) {
    console.error('Error fetching contact associations:', error.message);
    return Object.fromEntries(meetingIds.map((id) => [id, null])); // Return null for all IDs on error
  }
};

/**
 * Fetches emails for a batch of unique contact IDs using HubSpot's Batch Read API.
 * @param {string[]} contactIds - Array of unique contact IDs.
 * @returns {Promise<Object>} A map of contact IDs to their associated emails.
 */
const fetchContactEmailsBatchV3 = async (contactIds) => {
  if (!contactIds.length) return {}; // Early exit for empty contact list

  try {
    const response = await hubspotClient.apiRequest({
      method: 'POST',
      path: '/crm/v3/objects/contacts/batch/read',
      body: {
        properties: ['email'],
        inputs: contactIds.map((id) => ({ id })),
      },
    });

    const jsonBody = await response.json();

    // Map contact ID to email address
    return Object.fromEntries(
      jsonBody?.results
        ?.filter((contact) => contact.properties?.email)
        .map((contact) => [contact.id, contact.properties.email])
    );
  } catch (error) {
    console.error('Error fetching contact emails:', error.message);
    return {}; // Return empty map on error
  }
};

//#endregion


module.exports = pullDataFromHubspot;
