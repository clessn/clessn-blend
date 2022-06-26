# l_agoraplus_pressreleases_qc

## Purpose
This loader is used to retrieve the press releases from the Quebec political parties that have been stored in the data lake and transform them into a data warehouse table.  The data lake items have been harvested by [e_agoraplus_pressreleases_qc](https://github.com/clessn/clessn-blend/blob/main/Pipelines/Extractors/e_agoraplus-pressreleases-qc)

It is the second step of a pipeline that was created in order to collect data for the 2022 provincial elections.  The parties (and their web site urls) collected are:
* Coalition Avenir Quebec (CAQ): https://coalitionavenirquebec.org/fr/actualites/
* Parti Libéral du Québec (PLQ): https://plq.org/fr/communiques-de-presse/
* Québec Solidaire (QS): https://api-wp.quebecsolidaire.net/feed?post_type=articles&types=communiques-de-presse
* Parti Conservateur du Québec (PCQ): https://www.conservateur.quebec/communiques
* Parti Québecois (PQ): https://pq.org/nouvelles/

## Input
The data lake items are mostly **html** files except for QS for which **json** files are collect from the RSS feed.

The loader takes all data lake items in the [political_party_press_releases](https://clhub.clessn.cloud/admin/core/lake/?path=political_party_press_releases) path having the following meta data values
```
{
  "country": "CAN",
  "content_type": "political_party_press_release",
  "storage_class": "lake",
  "province_or_state": "QC"
}
```

## Output
And it loads the 
* date
* party
* country
* title
* content

...of each press release selected in the lake into a warehouse table named `warehouse_political_parties_press_releases`.

## Development methodology and containerization
This is based on the [retl](https://github.com/clessn/retl) repository.
See the [README.md](https://github.com/clessn/retl/blob/master/README.md) of the retl repo for more details.
