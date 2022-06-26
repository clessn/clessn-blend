# e_agoraplus_pressreleases_qc

## Purpose
This extractor is used to retrieve the press releases from the Quebec political parties.  It was created in order to collect data for the 2022 provincial elections.  The parties (and their web site urls) collected are:
* Coalition Avenir Quebec (CAQ): https://coalitionavenirquebec.org/fr/actualites/
* Parti Libéral du Québec (PLQ): https://plq.org/fr/communiques-de-presse/
* Québec Solidaire (QS): https://api-wp.quebecsolidaire.net/feed?post_type=articles&types=communiques-de-presse
* Parti Conservateur du Québec (PCQ): https://www.conservateur.quebec/communiques
* Parti Québecois (PQ): https://pq.org/nouvelles/

## Input of the extractor : Data, Path and Meta data
The data collected is stored in the data lake of the CLESSN in the [political_party_press_releases](https://clhub.clessn.cloud/admin/core/lake/?path=political_party_press_releases) path

The data collected is mostly **html** files except for QS for which **json** files are collect from the RSS feed.

The metada on the collected items is as follows
```
{
  "url": <the url of the press release extracted>,
  "format": <"html" or "json">,
  "country": "CAN",
  "hashtags": "elxn-qc2022, vitrine_democratique, polqc",
  "description": "Communiqués de presse des partis politiques",
  "content_type": "political_party_press_release",
  "storage_class": "lake",
  "political_party": <acronym of the political party for this press release">,
  "province_or_state": "QC"
}
```

## Output of the extractor

## Development methodology and containerization
This is based on the [retl](https://github.com/clessn/retl) repository.
See the [README.md](https://github.com/clessn/retl/blob/master/README.md) of the retl repo for more details.
