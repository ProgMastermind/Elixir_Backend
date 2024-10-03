# Use an official Elixir runtime as a parent image
FROM elixir:1.16-alpine

# Set the working directory in the container
WORKDIR /app

# Install build dependencies
RUN apk add --no-cache build-base

# Copy the mix.exs and mix.lock files
COPY mix.exs mix.lock ./

# Install project dependencies
RUN mix do deps.get, deps.compile

# Copy the rest of the application code
COPY . .

# Compile the project
RUN mix do compile

# Set the environment to production
ENV MIX_ENV=prod

# Expose the port the app runs on
EXPOSE 3001

# Set the default command to run when the container starts
CMD ["mix", "run", "--no-halt"]
