# Prétraitement des données pour l'annotateur OCRx
Données utilisés présentement: DailyMed

# Pour rouler localement
Assurez vous d'avoir installer `jupyter`, `jupyter-lab`, `pandas` et `numpy`.
Ensuite, ouvrez la ligne de commande et roulez
```bash
jupyter lab
```

Ceci ouvrait un onglet chez localhost:8888 ou localhost:8889 et vous pouvez continuer le développement sur `parser_notebook.ipynb`

# Description des fichiers
- `ocrx-data/` : Ceci contient les fichiers de l'ontologie OCRx séparé en forme, route d'administration, et ingrédient actif.
- `results/` : Ceci contient les résultats du matching/alignement entre les produits du DailyMed et d'OCRx pour les formes, les ROA et les ingrédients actifs.
- `all_drugs_ade_indications.csv` : Ceci est le fichier principal qui contient les effets secondaires et indications thérapeutiques du DailyMed. 
