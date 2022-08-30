# <e|l|r>_name_of_the_pipeline_script

## Purpose
This `[extractor|loader|refiner]` is used to blah blah blah.  

It was created in order to blah blah blah.  

## Input
```
[Short intro describing the input, the nature of the data and where it is located in simple words]

Example : 
```

The data collected comes from the political parties web sites.  
It is mostly **html** files except for QS for which **json** files are collect from the RSS feed.

```
[Provide a detailed description of the input data]

Example:
```

  The parties (and their web site urls) collected are:
  * Coalition Avenir Quebec (CAQ): https://coalitionavenirquebec.org/fr/actualites/
  * Parti Libéral du Québec (PLQ): https://plq.org/fr/communiques-de-presse/
  * Québec Solidaire (QS): https://api-wp.quebecsolidaire.net/feed?post_type=articles&types=communiques-de-presse
  * Parti Conservateur du Québec (PCQ): https://www.conservateur.quebec/communiques
  * Parti Québecois (PQ): https://pq.org/nouvelles/

```
[Specify the path of the data lake (if applicable)]

Example:
```

  The loader takes all data lake items in the [political_party_press_releases](https://clhub.clessn.cloud/admin/core/lake/?path=political_party_press_releases) path having the following meta data values

```
[Specify which metadata is being used at which value, in order to bulk select the data from its source]

Example:
```

The following meta data values are used in order to select all data like items from the data lake in the path 
```
  {
    "country": "CAN",
    "content_type": "political_party_press_release",
    "storage_class": "lake",
    "province_or_state": "QC"
  }
```

## Output
```
[Descripe the output data in simple terms]

Example:
```

The output are data lake items which are mostly **html** files except for QS for which **json** files are collect from the RSS feed

The extractor creates all data lake items in the [political_party_press_releases](https://clhub.clessn.cloud/admin/core/lake/?path=political_party_press_releases) path having the following meta data values:
```
  {
    "url": "<URL of the press release>",
    "format": "html|json",
    "country": "CAN",
    "hashtags": "elxn-qc2022, vitrine_democratique, polqc",
    "description": "Communiqués de presse des partis politiques",
    "content_type": "political_party_press_release",
    "storage_class": "lake",
    "political_party": "<Political parti for this press release>",
    "province_or_state": "QC"
  }
```

## Development methodology and containerization
This is based on the [retl](https://github.com/clessn/retl) repository.
See the [README.md](https://github.com/clessn/retl/blob/master/README.md) of the retl repo for more details.
