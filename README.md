# UniversalEnterprisePlatform

A Universal Enterprise Platform that can serve any type of organizations.

## Run Phoenix Server

To start your Phoenix server:

- Install dependencies with `mix deps.get`
- Create new .env file with `cp .env.example .env`
- Generate `SIGNING_SALT` value by running `mix phx.gen.secret 32`
- Put the `DB_USER`, `DB_PASSWORD` and `SIGNING_SALT` values in `.env`
- Load the `.env` values with `source .env`
- Create and migrate your database with `mix ecto.setup`
- Install Node.js dependencies with `npm install` inside the `assets` directory or remain in project dir and run `npm install --prefix assets`
- Start Phoenix endpoint with `mix phx.server` or inside IEx with `iex -S mix phx.server`

Now you can visit [`localhost:4000`](http://localhost:4000) from your browser.

Ready to run in production? Please [check our deployment guides](https://hexdocs.pm/phoenix/deployment.html).

## Learn more

* Official website: https://www.phoenixframework.org/
* Guides: https://hexdocs.pm/phoenix/overview.html
* Docs: https://hexdocs.pm/phoenix
* Forum: https://elixirforum.com/c/phoenix-forum
* Source: https://github.com/phoenixframework/phoenix
