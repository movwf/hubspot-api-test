import { Client } from "@hubspot/api-client";

const clientCache = {};

/**
 * Creates singleton hubspot client
 * @param {import('@hubspot/api-client/lib/src/configuration/IConfiguration').IConfiguration} config
 * @returns {Client} Hubspot Client
 */
const getHubspotClient = config => {
  const configKey = JSON.stringify(config || {});

  if (!clientCache[configKey]) {
    clientCache[configKey] = new Client(config);
  }

  return clientCache[configKey];
};

export default getHubspotClient;
