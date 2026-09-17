# AWS's own Lambda base image. It bundles the Runtime Interface Emulator, which
# serves the Lambda invoke API on :8080 -- the URL MinIO's webhook target calls.
FROM public.ecr.aws/lambda/nodejs:24

WORKDIR ${LAMBDA_TASK_ROOT}

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci

COPY src ./src
RUN npx tsc && npm prune --omit=dev

# One image, one handler per service: compose picks it with `command:`,
# the same way a Lambda container image's CMD names its handler.
CMD ["dist/metadata.handler"]
