# Use Node.js as the base image
FROM node:20

# Set working directory inside the container
WORKDIR /usr/src/app

# Install dependencies for Playwright
RUN apt-get update && apt-get install -y \
  libnss3 \
  libnspr4 \
  libdbus-1-3 \
  libatk1.0-0 \
  libatk-bridge2.0-0 \
  libcups2 \
  libdrm2 \
  libatspi2.0-0 \
  libxcomposite1 \
  libxdamage1 \
  libxfixes3 \
  libxrandr2 \
  libgbm1 \
  libxkbcommon0 \
  libasound2

RUN npm install -g node-ts
# Install pnpm
RUN npm install -g pnpm

# Copy package.json and package-lock.json to the working directory
COPY package*.json ./

# Install dependencies
RUN pnpm install

# Copy the rest of the application files to the working directory
COPY . .


CMD ["npm", "run", "start:prod"]