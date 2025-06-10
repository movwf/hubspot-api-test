/**
 * @typedef {Array<'contacts'|'companies'|'meetings'>} HubspotObjectName
 */

/**
 * @typedef {Object} HubspotAccount
 * @property {string} hubId
 * @property {string} hubDomain
 * @property {string} accessToken
 * @property {string} refreshToken
 * @property {Date} lastPulledDate
 * @property {Object} lastPulledDates
 * @property {Date} lastPulledDates.companies
 * @property {Date} lastPulledDates.contacts
 * @property {Date} lastPulledDates.deals
 */

/**
 * @typedef {Object} HubspotIntegration
 * @property {boolean} status
 * @property {HubspotAccount[]} accounts
 */

/**
 * @typedef {Object} Integrations
 * @property {HubspotIntegration} hubspot
 */

/**
 * @typedef {Object} MailPreferences
 * @property {boolean} weeklyReport
 */

/**
 * @typedef {Object} Customer
 * @property {string} customerId - MongoDB ObjectId reference to Customer
 * @property {MailPreferences} mailPreferences
 * @property {('creator'|'admin'|'member'|'viewer')} accessLevel
 */

/**
 * @typedef {Object} Company
 * @property {string} name
 * @property {string} website
 */

/**
 * @typedef {Object} Domain
 * @property {Customer[]} customers
 * @property {string} customer - MongoDB ObjectId reference to Customer
 * @property {Company} company
 * @property {string} apiKey
 * @property {string} customerDBName
 * @property {boolean} setup
 * @property {Integrations} integrations
 */

export default {};
