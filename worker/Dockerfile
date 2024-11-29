# Base image
FROM node:18

# Set working directory
WORKDIR /usr/src/app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy application code
COPY . .

# Build TypeScript code
RUN npm run build

# Expose application port (if needed, optional for workers)
EXPOSE 8080

# Default command
CMD ["node", "dist/index.js"]
