# Stage 1: build all three binaries with the full Go toolchain.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Download dependencies first, in their own layer: this layer is reused from
# cache as long as go.mod/go.sum don't change, even when the code does.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 produces fully static binaries that need no C library, so they
# run on any minimal image. -trimpath and -s -w drop local paths and debug
# symbols to make the binaries smaller.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Stage 2: a small runtime image containing only the binaries.
# Alpine (not distroless) keeps a shell, so you can `docker compose exec cs1 ls /data`.
FROM alpine:3.20
COPY --from=build /out/coordinator /out/chunkserver /out/gfs /
