const fetch = require("node-fetch");

class Elasticsearch {
  config;

  constructor(config) {
    this.config = config;
  }

  async search(request) {
    const response = await fetch(`http://${this.config.address}:${this.config.port}/${this.config.indexPrefix}*/_search`, {
      method: 'post',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request, null, 2),
    });

    if (response.status >= 400)
      throw new Error(await response.text());

    return await response.json();
  }
}

module.exports = Elasticsearch;
