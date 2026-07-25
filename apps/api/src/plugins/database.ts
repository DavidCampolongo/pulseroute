import { createDatabaseClient, type DatabaseClient } from "@pulseroute/db";
import fastifyPlugin from "fastify-plugin";

declare module "fastify" {
  interface FastifyInstance {
    db: DatabaseClient;
  }
}

type DatabasePluginOptions = {
  databaseUrl: string;
};

export const databasePlugin = fastifyPlugin<DatabasePluginOptions>(
  async (app, options) => {
    const databaseClient = createDatabaseClient(options.databaseUrl);

    app.decorate("db", databaseClient);

    app.addHook("onClose", async () => {
      await databaseClient.$disconnect();
    });
  },
  {
    name: "database",
  },
);
