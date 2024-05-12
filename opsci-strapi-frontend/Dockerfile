# Utiliser une image Node.js compatible avec votre projet Vite/React
FROM node:21-alpine

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier tous les fichiers du frontend dans le conteneur

COPY . .
# Installer les dépendances
RUN yarn install && yarn build



# Exposer le port sur lequel l'application va écouter (remplacez par le port correct si nécessaire)
EXPOSE 5173

# Commande pour exécuter l'application en mode développement au démarrage du conteneur
CMD ["yarn", "dev","--host", "0.0.0.0"]
