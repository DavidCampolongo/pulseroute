import { Prisma, type DatabaseClient } from "@pulseroute/db";

type DatabaseClockRow = {
  databaseNow: Date;
};

export async function readDatabaseNow(database: DatabaseClient): Promise<Date> {
  const rows = await database.$queryRaw<DatabaseClockRow[]>(Prisma.sql`
    SELECT clock_timestamp() AS "databaseNow"
  `);
  const databaseNow = rows[0]?.databaseNow;

  if (!(databaseNow instanceof Date) || Number.isNaN(databaseNow.getTime())) {
    throw new Error("PostgreSQL did not return a valid database clock value");
  }

  return databaseNow;
}
