# Template de projet R

## Features
* réutilisable
* reproductible
* dockerizable

## Nécessités locales
* [R](https://www.r-project.org/)
* [python3](https://www.python.org/)
* [poetry](https://poetry.eustace.io/)
* [renv](https://rstudio.github.io/renv/index.html)

## Rouler le serveur
Solution 1:
```sh
poetry run python pipeline/server.py
```

Solution 2, avec variables d'environnement:
```
poetry shell
export MA_VARIABLE1=ma_valeur1
export MA_VARIABLE2=ma_valeur2
python pipeline/server.py
```

## Utilisation
* Commencez par copier le contenu de ce repo dans un dossier de votre choix.
* Supprimez si nécessaire les fichiers `code.py` et `code.R` dans le sous-dossier `code`; Ils y sont à titre d'exemple. Notez par contre comment on y extrait les variables d'environnement pour chaque langage. Ce pourrait être pertinent plus tard.
* en restant dans le sous-dossier `code/`, développez votre pipeline. Vous pouvez utiliser la fonction `print` pour des cas de débogage.
* Déterminez les valeurs sensibles dont vous aurez besoin et qui ne devraient pas se retrouver dans le repo. Nous construiront une requête à partir de ça.
* Déterminez le ou les fichiers, dans l'ordre, qui devront être exécutés.
* Pour tester, vous avez deux choix:
  * Lancer le server avec poetry et utiliser curl ou un client http (port 8080)
  * Construire et rouler l'image docker (ou avec le docker-compose) et utiliser curl ou un client http (port 8001)

## Construction de requête (exemple)
```json
{
  // commandes cli à exécuter
	"commands": ["python3", "code/code.py", "&&",
							 "Rscript", "code/code.R"],
	// variables d'environnement à configurer
  // notez que si vous configurez PATH, sa valeur sera ajoutée au PATH existant du conteneur/environnement
  "env_vars":
	{
		"clhub_username": "admin",
		"clhub_password": "secret",
		"clhub_url": "http://localhost:8080"
	}
}
```

## Endpoints du serveur
* `POST /run`: avec une requête en JSON, lance l'exécution du pipeline, ou retourne 400 si le pipeline roule déjà
* `GET /status`: retourne l'état du pipeline
* `GET /history`: retourne l'historique cli des dernières exécutions

Valeur de `status` possible:
* `running`: le pipeline est en cours d'exécution. On ne peut donc pas lancer d'exécution.
* `idle`: le pipeline n'a jamais encore roulé. On peut donc lancer une exécution.
* `error`: le pipeline a rencontré une erreur et est arrêté. Il peut être relancé.
* `success`: le pipeline a terminé avec succès. Il peut être relancé.
* `starting`: lorsqu'on lance une exécution, ce statut est retourné par `/run`.

### Recettes CURL pour tester le serveur

```sh
curl -X POST -H "Content-Type: application/json" -d '{"commands": ["Rscript", "code/code.R"], "env_vars": {}}' http://localhost:8080/run > output.json

curl -X GET http://localhost:8080/status > output.json

```


## Notes sur RENV
On utilise ici le package `renv`, qui permet d'installer localement les packages nécessaires et de noter les versions exactes pour  (du mieux qu'on peut) la reproductibilité sur un autre environnement. Il s'agit donc d'une habitude de travail à prendre, soit de "repartir à zéro" pour chaque projet et de déterminer les packages minimaux nécessaires. En partageant nos projets et le fichier `renv.lock`, un autre utilisateur peut installer une version locale de chaque package R de la même version que vous afin de s'assurer que l'exécution du code est similaire. C'est aussi une belle façon de s'assurer que tous ont les bons packages avant de tomber sur une erreur.


### Comment m'utiliser
* s'assurer d'avoir installé `renv` avec `install.packages("renv")`
* exécuter `renv::restore()` pour générer le dossier `renv` et installer les packages nécessaires automatiquement
* sélectionner l'option y si on vous le demande
* s'il y a un message d'avertissement comme quoi les versions de R ne concordent pas, ce n'est pas réellement grave. Dans le cadre d'un nouveau projet, vous devriez plutôt installer une version plus récente de votre R.
* déterminer les packages R qui seront nécessaires
* s'assurer que RStudio détecte l'environnement renv plutôt que l'environnement global
  * RStudio devrait n'afficher que renv comme package installé.
  * Si vous trouvez vos packages comme dplyr ou autre, c'est que l'environnement ne fonctionne pas, redémarrez RStudio
* dans la console R, installer les packages nécessaires un après l'autre
* taper `renv::snapshot()`; renv stockera les packages et les versions que vous avez installé
* parfois, certains packages ne sont pas appliqués au fichier renv.lock. Validez son contenu et utilisez `renv::record("package")` pour enregistrer les packages qui ne sont pas appliqués.
* développer votre projet. À chaque fois que vous devez installer un nouveau package ou en retirer un, assurez vous de `renv::snapshot()` pour mettre à jour renv.lock
* Tester votre projet final en roulant `Rscript main.R`, ce qui roulera votre script en entier (l'équivalent de sourcer). Si votre script fonctionne, on pourra dockeriser et le publier.


### Aide-mémoire
```R
renv::init() # créer un environnement renv de toute pièce,
# insi que le .Rprofile
# ce n'Est pas nécessaire si un renv.lock et un
# .Rprofile sont déjà présent

# .Rprofile est un fichier qui est détecté par
# R (et RStudio) et qui exécute le code à chaque
# démarrage de RStudio (ou d'une console R)

renv::snapshot()
# j'ai installé plusieurs packages et je veux mettre à jour renv.lock

renv::restore()
# j'ai téléchargé un repo avec un renv.lock et
# un .Rprofile, et j'aimerais avoir un environnement
# renv identique à celui du développeur

renv::restore()
# s'applique ausi si j'ai installé un package
# sans faire snapshot(), mais que j'aimerais
# rapidement revenir à l'état précédent

# si on vous a fourni un renv.lock et rien
# d'autre (pas de .Rprofile, par exemple),
# vous pouvez renv::init(), puis remplacer le renv.lock par le vôtre.
```
