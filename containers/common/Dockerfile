# Base image
FROM node:23

# Set working directory
WORKDIR /usr/src/app

# Copy shared dependencies
COPY common/package*.json ./
RUN npm install


# Copy container-specific code
ARG CONTAINER_NAME
COPY ${CONTAINER_NAME}/index.ts ./src/index.ts
# Copy shared code
COPY common/*.ts ./src

# Copy TypeScript config
COPY common/tsconfig.json ./

# Build TypeScript code
RUN npm run build --verbose

# Verify build output
# RUN ls -l dist/

# Expose application port (if needed, optional for workers)
EXPOSE 8080

# Default command
CMD ["node", "dist/index.js"]