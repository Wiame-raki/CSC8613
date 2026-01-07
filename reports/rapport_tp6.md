
# Exercice 1:

![simple](tp6_1.png) 
![simple](tp6_2.png) 

# Exercice 2:
![simple](tp6_3.png) 
Extraire une fonction pure isole la logique décisionnelle de l'infrastructure, ce qui rend les tests unitaires totalement déterministes et instantanés car il n'est plus nécessaire de simuler (mocker) des dépendances externes complexes comme une base de données ou un registre de modèles.

# Exercice 3:
![simple](tp6_4.png) 
![simple](tp6_5.png) 
*J'ai plusieurs version None après la version en production car j'ai relancer le code plusieurs fois .*
On utilise un delta afin d’éviter de promouvoir un nouveau modèle pour des gains de performance trop faibles ou dus au hasard, et ainsi garantir que toute mise en production apporte une amélioration significative et stable par rapport au modèle existant.

# Exercice 4:
![simple](tp6_6.png) 
![simple](tp6_7.png)

# Exercice 5: 
![simple](tp6_8.png)
L’API doit être redémarrée car le modèle MLflow est chargé une seule fois au démarrage à partir du registre (stage *Production*), et une promotion de modèle n’est donc prise en compte qu’après un redémarrage permettant de recharger la nouvelle version en mémoire.



