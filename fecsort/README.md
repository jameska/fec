# Tri de fichier FEC

Le fichier FEC (Fichier des Écritures Comptables) est un fichier à plat.
Le fichier est structuré comme suit:
- Entête
- Données

L'entête donne le nom des colonnes, ils sont séparés par un séparateur.
Leq séparateurs possible sont le pipe (|) ou la tabulation(\t).

Les encodages de caractères autorisés sont UTF-8 et ISO-8859-15.

Le séparateur de décimale est la virgule (,).
Il n'y a pas de séparateur de millier.

Les dates sont au format yyyyMMdd.

Les séparateurs de fin de ligne sont
- CR / LF
- LF

Le fcihier trié comporte une colonne suplémentaire appelé +index

La donnée est codé en base64
 - 4 caractères pour les lignes avant la 16.777.216e  
 - 8 caractères pour les suivantes avant la 281.474.976.710.656e
 - 12 caractères pour les suivants

Quelques statisqtiques

| taille |taillie initiale | taille finale | nombre de lignes | nombre de chunks | temps |
|---------|---------------:|------------------:|------------------:|------------:|
| 100Go  | 100 873 130 310 | | 615 194 600 | 505 |  |
| 10Go  | 10 087 313 220 | 10 573 879 506 | 61 519 460 | 51 | 32m 52s |
| 2Go  | 2 005 158 919 | 2 078 982 278 | 12 303 892 | 11 | 13m 27s |




pour un fichier de 100Go

- taille du fichier initial : 100 873 130 310
- taille du fichier trié :
- nombre de lignes : 615 194 600
- nombre de chunk (200.000.000 octets): 505
- temps de traitement :

pour un fichier de 10Go

- taille du fichier initial : 10 087 313 220
- taille du fichier trié : 10 573 879 506
- nombre de lignes : 61 519 460
- nombre de chunk (200.000.000 octets): 51
- temps de traitement : 32' 52"

pour un fichier de 2Go

- taille du fichier initial : 2 005 158 919
- taille du fichier trié    : 2 078 982 278
- nombre de lignes : 12 303 892
- nombre de chunk (200.000.000 octets): 11
- temps de traitement : 13' 27"
- 

