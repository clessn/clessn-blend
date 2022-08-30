# Template de pipeline


## Nécessités pour tests pré-hublot
* [R](https://www.r-project.org/)
* [python3](https://www.python.org/)
* [poetry](https://poetry.eustace.io/)
* [renv](https://rstudio.github.io/renv/index.html)
* docker et docker compose

## Étapes
* Copiez ce projet dans votre dossier de travail
* Renommez ce README.md en autre chose, puis renommez template_README.md en README.md
* Développez vos scripts dans le dossier code/
* Testez vos scripts en exécutant à partir de la racine de ce projet, profitant de ce fait de poetry (si dev en python) et de renv (si dev en R)
* Déterminez le nom de votre pipeline (supposons `media_transformer`)
* Ouvrez le fichier run.py, et notez les commentaires TODO. Appliquez au besoin.
  * assurez-vous que `command = "Rscript code/code.R"` pointe vers le fichier à exécuter pour cette tâche
  * Notez que vous pouvez créer des tâches supplémentaires dans le fichier run.py
  * supprimez les lignes contenant TODO au besoin

## Tests pré-hublot
* ouvrez le fichier test.py
  * modifiez `"run.task1"` au besoin.Notez le prefix `run.` qui représente le nom du fichier dans laquelle la tâche se trouve, `run.py`. Si plus haut, vous avez renommé `task1` en `ma_tache`, on devrait alors voir ici `run.ma_tache`
  * modifiez `queue="my-queue"` pour `queue="media-transformer"`. La queue doit être unique à chaque pipeline. Utilisez donc le nom de votre pipeline.
* Ouvrez docker-compose.yml, et modifiez `--queues=my-queue` pour `--queues=media-transformer`

La queue permet de déterminer quel conteneur executera la tâche demandée. Deux conteneurs ayant la même queue `my-queue` et la même tâche `task1` exécuteront aléatoirement la même tache. Si ces deux conteneurs ont du code différent pour `task1`, il est alors impossible de déterminer quel code exécuter. D'où l'importance d'une queue unique pour chaque pipeline. Il est aussi possible, pour des pipelines lourds, de multiplier le nombre de conteneurs sous la même queue pour exécuter en parallel.

Pour tester votre pipeline sous docker:
* docker compose build
* docker compose up
* votre pipeline est maintenant prêt à être exécuté.
* dans une autre console, exécutez `poetry run python test.py`

Au besoin, vous pouvez modifier test.py pour tester différentes tâches, ajouter différentes variables d'environnement, etc.


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
