FROM gingersociety/rust-rocket-api-builder:latest as builder

ARG GINGER_TOKEN

# Create a new directory for the app
WORKDIR /app
COPY . .
# Run the ginger-auth command and capture the output
RUN ginger-auth token-login $GINGER_TOKEN
RUN ginger-connector connect prod
# Build the application in release mode
RUN cargo build --release

# Second stage: Create the minimal runtime image
FROM gingersociety/rust-rocket-api-runner:latest

RUN apt-get update && apt-get install -y \
    curl

# Install kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/GingerPresence /app/

# Set the working directory
WORKDIR /app

# Run the executable when the container starts
ENTRYPOINT ["./GingerPresence"]