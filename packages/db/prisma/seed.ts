import { PrismaPg } from "@prisma/adapter-pg";
import { config as loadEnv } from "dotenv";
import * as argon2 from "argon2";
import { fileURLToPath } from "node:url";
import { Pool } from "pg";

import {
  AssignmentStatus,
  OperatorStatus,
  PrismaClient,
  ServiceRequestPriority,
  ServiceRequestStatus,
  UserRole,
} from "../src/generated/prisma/client";

loadEnv({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const seedUserPassword = process.env.SEED_USER_PASSWORD;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required to run the development seed.");
}

if (!seedUserPassword || seedUserPassword.length < 12) {
  throw new Error(
    "SEED_USER_PASSWORD must be configured locally and contain at least 12 characters.",
  );
}

const pool = new Pool({
  connectionString: databaseUrl,
});

const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

function stableId(group: number, sequence: number): string {
  return `${String(group).padStart(8, "0")}-0000-4000-8000-${String(
    sequence,
  ).padStart(12, "0")}`;
}

const organization = {
  id: stableId(1, 1),
  name: "PulseRoute Development",
} as const;

const users = [
  {
    id: stableId(2, 1),
    email: "admin@pulseroute.local",
    role: UserRole.ADMIN,
  },
  {
    id: stableId(2, 2),
    email: "dispatcher@pulseroute.local",
    role: UserRole.DISPATCHER,
  },
  {
    id: stableId(2, 3),
    email: "viewer@pulseroute.local",
    role: UserRole.VIEWER,
  },
] as const;

const skills = [
  { id: stableId(3, 1), name: "General Support" },
  { id: stableId(3, 2), name: "Account Access" },
  { id: stableId(3, 3), name: "Billing" },
  { id: stableId(3, 4), name: "API Integration" },
  { id: stableId(3, 5), name: "Data Migration" },
  { id: stableId(3, 6), name: "Incident Response" },
  { id: stableId(3, 7), name: "Security Review" },
  { id: stableId(3, 8), name: "Healthcare Compliance" },
  { id: stableId(3, 9), name: "Spanish Support" },
  { id: stableId(3, 10), name: "Enterprise Escalation" },
] as const;

type SkillName = (typeof skills)[number]["name"];

type OperatorSeed = {
  id: string;
  name: string;
  status: OperatorStatus;
  region: string;
  maxConcurrentAssignments: number;
  skills: Array<{
    name: SkillName;
    level: number;
  }>;
};

const operators: OperatorSeed[] = [
  {
    id: stableId(4, 1),
    name: "Ava Brooks",
    status: OperatorStatus.AVAILABLE,
    region: "NORTH",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "General Support", level: 5 },
      { name: "Account Access", level: 4 },
      { name: "Billing", level: 1 },
    ],
  },
  {
    id: stableId(4, 2),
    name: "Liam Chen",
    status: OperatorStatus.AVAILABLE,
    region: "NORTH",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "API Integration", level: 5 },
      { name: "Data Migration", level: 4 },
      { name: "Incident Response", level: 2 },
    ],
  },
  {
    id: stableId(4, 3),
    name: "Maya Patel",
    status: OperatorStatus.AVAILABLE,
    region: "NORTH",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "General Support", level: 4 },
      { name: "Spanish Support", level: 5 },
      { name: "Account Access", level: 3 },
    ],
  },
  {
    id: stableId(4, 4),
    name: "Noah Williams",
    status: OperatorStatus.UNAVAILABLE,
    region: "NORTH",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "Incident Response", level: 5 },
      { name: "Security Review", level: 4 },
    ],
  },
  {
    id: stableId(4, 5),
    name: "Sofia Rivera",
    status: OperatorStatus.AVAILABLE,
    region: "NORTH",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Billing", level: 4 },
      { name: "Spanish Support", level: 5 },
      { name: "General Support", level: 3 },
    ],
  },
  {
    id: stableId(4, 6),
    name: "Ethan Kim",
    status: OperatorStatus.AVAILABLE,
    region: "SOUTH",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "General Support", level: 4 },
      { name: "Account Access", level: 5 },
      { name: "Billing", level: 4 },
    ],
  },
  {
    id: stableId(4, 7),
    name: "Isabella Moore",
    status: OperatorStatus.AVAILABLE,
    region: "SOUTH",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "API Integration", level: 4 },
      { name: "Data Migration", level: 5 },
    ],
  },
  {
    id: stableId(4, 8),
    name: "Lucas Johnson",
    status: OperatorStatus.UNAVAILABLE,
    region: "SOUTH",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Healthcare Compliance", level: 5 },
      { name: "General Support", level: 4 },
    ],
  },
  {
    id: stableId(4, 9),
    name: "Mia Thompson",
    status: OperatorStatus.AVAILABLE,
    region: "SOUTH",
    maxConcurrentAssignments: 4,
    skills: [
      { name: "General Support", level: 5 },
      { name: "Billing", level: 3 },
      { name: "Spanish Support", level: 4 },
    ],
  },
  {
    id: stableId(4, 10),
    name: "Oliver Davis",
    status: OperatorStatus.INACTIVE,
    region: "SOUTH",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Security Review", level: 5 },
      { name: "Incident Response", level: 4 },
    ],
  },
  {
    id: stableId(4, 11),
    name: "Amelia Wilson",
    status: OperatorStatus.AVAILABLE,
    region: "EAST",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "API Integration", level: 5 },
      { name: "Security Review", level: 4 },
      { name: "Incident Response", level: 3 },
    ],
  },
  {
    id: stableId(4, 12),
    name: "James Anderson",
    status: OperatorStatus.AVAILABLE,
    region: "EAST",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "Data Migration", level: 5 },
      { name: "API Integration", level: 4 },
      { name: "General Support", level: 3 },
    ],
  },
  {
    id: stableId(4, 13),
    name: "Harper Thomas",
    status: OperatorStatus.UNAVAILABLE,
    region: "EAST",
    maxConcurrentAssignments: 1,
    skills: [
      { name: "Account Access", level: 5 },
      { name: "General Support", level: 4 },
    ],
  },
  {
    id: stableId(4, 14),
    name: "Benjamin Taylor",
    status: OperatorStatus.AVAILABLE,
    region: "EAST",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Billing", level: 5 },
      { name: "Account Access", level: 3 },
      { name: "General Support", level: 3 },
    ],
  },
  {
    id: stableId(4, 15),
    name: "Evelyn Martinez",
    status: OperatorStatus.AVAILABLE,
    region: "EAST",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "Spanish Support", level: 5 },
      { name: "General Support", level: 4 },
      { name: "Billing", level: 3 },
    ],
  },
  {
    id: stableId(4, 16),
    name: "Henry Jackson",
    status: OperatorStatus.AVAILABLE,
    region: "WEST",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "API Integration", level: 4 },
      { name: "Data Migration", level: 4 },
      { name: "Security Review", level: 3 },
    ],
  },
  {
    id: stableId(4, 17),
    name: "Charlotte White",
    status: OperatorStatus.AVAILABLE,
    region: "WEST",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "General Support", level: 5 },
      { name: "Account Access", level: 4 },
    ],
  },
  {
    id: stableId(4, 18),
    name: "Alexander Harris",
    status: OperatorStatus.UNAVAILABLE,
    region: "WEST",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Incident Response", level: 5 },
      { name: "Security Review", level: 5 },
    ],
  },
  {
    id: stableId(4, 19),
    name: "Ella Martin",
    status: OperatorStatus.AVAILABLE,
    region: "WEST",
    maxConcurrentAssignments: 1,
    skills: [
      { name: "Healthcare Compliance", level: 5 },
      { name: "General Support", level: 3 },
    ],
  },
  {
    id: stableId(4, 20),
    name: "Daniel Garcia",
    status: OperatorStatus.INACTIVE,
    region: "WEST",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Spanish Support", level: 5 },
      { name: "Billing", level: 4 },
    ],
  },
  {
    id: stableId(4, 21),
    name: "Scarlett Lee",
    status: OperatorStatus.AVAILABLE,
    region: "CENTRAL",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "General Support", level: 4 },
      { name: "Billing", level: 4 },
      { name: "Account Access", level: 4 },
    ],
  },
  {
    id: stableId(4, 22),
    name: "Matthew Clark",
    status: OperatorStatus.AVAILABLE,
    region: "CENTRAL",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "API Integration", level: 5 },
      { name: "Incident Response", level: 4 },
      { name: "Security Review", level: 4 },
    ],
  },
  {
    id: stableId(4, 23),
    name: "Grace Lewis",
    status: OperatorStatus.AVAILABLE,
    region: "CENTRAL",
    maxConcurrentAssignments: 2,
    skills: [
      { name: "Enterprise Escalation", level: 5 },
      { name: "Incident Response", level: 4 },
    ],
  },
  {
    id: stableId(4, 24),
    name: "Sebastian Walker",
    status: OperatorStatus.INACTIVE,
    region: "EAST",
    maxConcurrentAssignments: 1,
    skills: [
      { name: "Enterprise Escalation", level: 5 },
      { name: "Security Review", level: 4 },
    ],
  },
  {
    id: stableId(4, 25),
    name: "Chloe Hall",
    status: OperatorStatus.UNAVAILABLE,
    region: "CENTRAL",
    maxConcurrentAssignments: 3,
    skills: [
      { name: "Data Migration", level: 5 },
      { name: "API Integration", level: 4 },
      { name: "Healthcare Compliance", level: 4 },
    ],
  },
];

type ServiceRequestSeed = {
  id: string;
  externalId: string;
  requiredSkill: SkillName;
  status: ServiceRequestStatus;
  priority: ServiceRequestPriority;
  region: string;
};

const serviceRequests: ServiceRequestSeed[] = [
  {
    id: stableId(5, 1),
    externalId: "seed-load-healthcare-west-001",
    requiredSkill: "Healthcare Compliance",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.HIGH,
    region: "WEST",
  },
  {
    id: stableId(5, 2),
    externalId: "seed-load-enterprise-central-001",
    requiredSkill: "Enterprise Escalation",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.HIGH,
    region: "CENTRAL",
  },
  {
    id: stableId(5, 3),
    externalId: "seed-load-enterprise-central-002",
    requiredSkill: "Enterprise Escalation",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.NORMAL,
    region: "CENTRAL",
  },
  {
    id: stableId(5, 4),
    externalId: "seed-load-api-north-001",
    requiredSkill: "API Integration",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.NORMAL,
    region: "NORTH",
  },
  {
    id: stableId(5, 5),
    externalId: "seed-load-general-south-001",
    requiredSkill: "General Support",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.NORMAL,
    region: "SOUTH",
  },
  {
    id: stableId(5, 6),
    externalId: "seed-load-general-south-002",
    requiredSkill: "General Support",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.LOW,
    region: "SOUTH",
  },
  {
    id: stableId(5, 7),
    externalId: "seed-load-security-east-001",
    requiredSkill: "Security Review",
    status: ServiceRequestStatus.ASSIGNED,
    priority: ServiceRequestPriority.HIGH,
    region: "EAST",
  },
  {
    id: stableId(5, 8),
    externalId: "seed-history-billing-south-001",
    requiredSkill: "Billing",
    status: ServiceRequestStatus.CANCELLED,
    priority: ServiceRequestPriority.NORMAL,
    region: "SOUTH",
  },
  {
    id: stableId(5, 9),
    externalId: "seed-history-data-east-001",
    requiredSkill: "Data Migration",
    status: ServiceRequestStatus.CANCELLED,
    priority: ServiceRequestPriority.LOW,
    region: "EAST",
  },
  {
    id: stableId(5, 10),
    externalId: "seed-hard-healthcare-west-001",
    requiredSkill: "Healthcare Compliance",
    status: ServiceRequestStatus.PENDING,
    priority: ServiceRequestPriority.HIGH,
    region: "WEST",
  },
  {
    id: stableId(5, 11),
    externalId: "seed-hard-enterprise-north-001",
    requiredSkill: "Enterprise Escalation",
    status: ServiceRequestStatus.PENDING,
    priority: ServiceRequestPriority.HIGH,
    region: "NORTH",
  },
  {
    id: stableId(5, 12),
    externalId: "seed-normal-api-west-001",
    requiredSkill: "API Integration",
    status: ServiceRequestStatus.PENDING,
    priority: ServiceRequestPriority.NORMAL,
    region: "WEST",
  },
  {
    id: stableId(5, 13),
    externalId: "seed-normal-spanish-south-001",
    requiredSkill: "Spanish Support",
    status: ServiceRequestStatus.PENDING,
    priority: ServiceRequestPriority.NORMAL,
    region: "SOUTH",
  },
];

const assignments = [
  {
    id: stableId(6, 1),
    requestExternalId: "seed-load-healthcare-west-001",
    operatorName: "Ella Martin",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 2),
    requestExternalId: "seed-load-enterprise-central-001",
    operatorName: "Grace Lewis",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 3),
    requestExternalId: "seed-load-enterprise-central-002",
    operatorName: "Grace Lewis",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 4),
    requestExternalId: "seed-load-api-north-001",
    operatorName: "Liam Chen",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 5),
    requestExternalId: "seed-load-general-south-001",
    operatorName: "Mia Thompson",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 6),
    requestExternalId: "seed-load-general-south-002",
    operatorName: "Mia Thompson",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 7),
    requestExternalId: "seed-load-security-east-001",
    operatorName: "Amelia Wilson",
    status: AssignmentStatus.ACTIVE,
  },
  {
    id: stableId(6, 8),
    requestExternalId: "seed-history-billing-south-001",
    operatorName: "Ethan Kim",
    status: AssignmentStatus.CANCELLED,
  },
  {
    id: stableId(6, 9),
    requestExternalId: "seed-history-data-east-001",
    operatorName: "James Anderson",
    status: AssignmentStatus.CANCELLED,
  },
] as const;

const skillIdByName = new Map<SkillName, string>(
  skills.map((skill) => [skill.name, skill.id]),
);

const operatorIdByName = new Map(
  operators.map((operator) => [operator.name, operator.id]),
);

const requestIdByExternalId = new Map(
  serviceRequests.map((request) => [request.externalId, request.id]),
);

function requireSkillId(name: SkillName): string {
  const id = skillIdByName.get(name);

  if (!id) {
    throw new Error(`Missing seed skill: ${name}`);
  }

  return id;
}

function requireOperatorId(name: string): string {
  const id = operatorIdByName.get(name);

  if (!id) {
    throw new Error(`Missing seed operator: ${name}`);
  }

  return id;
}

function requireRequestId(externalId: string): string {
  const id = requestIdByExternalId.get(externalId);

  if (!id) {
    throw new Error(`Missing seed service request: ${externalId}`);
  }

  return id;
}

async function createPasswordHashes(): Promise<string[]> {
  const hashes = await Promise.all(
    users.map(() =>
      argon2.hash(seedUserPassword, {
        type: argon2.argon2id,
        memoryCost: 19456,
        timeCost: 2,
        parallelism: 1,
      }),
    ),
  );

  for (const hash of hashes) {
    const valid = await argon2.verify(hash, seedUserPassword);

    if (!valid) {
      throw new Error("Generated seed password hash failed verification.");
    }
  }

  return hashes;
}

async function main(): Promise<void> {
  const passwordHashes = await createPasswordHashes();

  const operatorSkillRows = operators.flatMap((operator, operatorIndex) =>
    operator.skills.map((skill, skillIndex) => ({
      id: stableId(7, operatorIndex * 10 + skillIndex + 1),
      organizationId: organization.id,
      operatorId: operator.id,
      skillId: requireSkillId(skill.name),
      level: skill.level,
    })),
  );

  const expectedOperatorSkillIds = operatorSkillRows.map((row) => row.id);

  const summary = await prisma.$transaction(
    async (transaction) => {
      await transaction.organization.upsert({
        where: {
          id: organization.id,
        },
        update: {
          name: organization.name,
        },
        create: organization,
      });

      for (const [index, user] of users.entries()) {
        const passwordHash = passwordHashes[index];

        if (!passwordHash) {
          throw new Error(`Missing password hash for ${user.email}`);
        }

        await transaction.user.upsert({
          where: {
            id: user.id,
          },
          update: {
            organizationId: organization.id,
            email: user.email,
            passwordHash,
            role: user.role,
          },
          create: {
            id: user.id,
            organizationId: organization.id,
            email: user.email,
            passwordHash,
            role: user.role,
          },
        });
      }

      for (const skill of skills) {
        await transaction.skill.upsert({
          where: {
            id: skill.id,
          },
          update: {
            organizationId: organization.id,
            name: skill.name,
          },
          create: {
            id: skill.id,
            organizationId: organization.id,
            name: skill.name,
          },
        });
      }

      for (const operator of operators) {
        await transaction.operator.upsert({
          where: {
            id: operator.id,
          },
          update: {
            organizationId: organization.id,
            name: operator.name,
            status: operator.status,
            region: operator.region,
            maxConcurrentAssignments: operator.maxConcurrentAssignments,
          },
          create: {
            id: operator.id,
            organizationId: organization.id,
            name: operator.name,
            status: operator.status,
            region: operator.region,
            maxConcurrentAssignments: operator.maxConcurrentAssignments,
          },
        });
      }

      await transaction.operatorSkill.deleteMany({
        where: {
          operatorId: {
            in: operators.map((operator) => operator.id),
          },
          id: {
            notIn: expectedOperatorSkillIds,
          },
        },
      });

      for (const operatorSkill of operatorSkillRows) {
        await transaction.operatorSkill.upsert({
          where: {
            id: operatorSkill.id,
          },
          update: {
            organizationId: operatorSkill.organizationId,
            operatorId: operatorSkill.operatorId,
            skillId: operatorSkill.skillId,
            level: operatorSkill.level,
          },
          create: operatorSkill,
        });
      }

      for (const request of serviceRequests) {
        await transaction.serviceRequest.upsert({
          where: {
            id: request.id,
          },
          update: {
            organizationId: organization.id,
            externalId: request.externalId,
            requiredSkillId: requireSkillId(request.requiredSkill),
            status: request.status,
            priority: request.priority,
            region: request.region,
          },
          create: {
            id: request.id,
            organizationId: organization.id,
            externalId: request.externalId,
            requiredSkillId: requireSkillId(request.requiredSkill),
            status: request.status,
            priority: request.priority,
            region: request.region,
          },
        });
      }

      for (const assignment of assignments) {
        await transaction.assignment.upsert({
          where: {
            id: assignment.id,
          },
          update: {
            organizationId: organization.id,
            serviceRequestId: requireRequestId(assignment.requestExternalId),
            operatorId: requireOperatorId(assignment.operatorName),
            status: assignment.status,
          },
          create: {
            id: assignment.id,
            organizationId: organization.id,
            serviceRequestId: requireRequestId(assignment.requestExternalId),
            operatorId: requireOperatorId(assignment.operatorName),
            status: assignment.status,
          },
        });
      }

      return {
        organizations: await transaction.organization.count(),
        users: await transaction.user.count({
          where: {
            organizationId: organization.id,
          },
        }),
        skills: await transaction.skill.count({
          where: {
            organizationId: organization.id,
          },
        }),
        operators: await transaction.operator.count({
          where: {
            organizationId: organization.id,
          },
        }),
        operatorSkills: await transaction.operatorSkill.count({
          where: {
            organizationId: organization.id,
          },
        }),
        serviceRequests: await transaction.serviceRequest.count({
          where: {
            organizationId: organization.id,
          },
        }),
        assignments: await transaction.assignment.count({
          where: {
            organizationId: organization.id,
          },
        }),
      };
    },
    {
      timeout: 60_000,
    },
  );

  console.log("PulseRoute development seed completed.");
  console.table(summary);

  console.log("Difficult routing fixtures:");
  console.log("- seed-hard-healthcare-west-001");
  console.log("- seed-hard-enterprise-north-001");
}

main()
  .then(async () => {
    await prisma.$disconnect();
    await pool.end();
  })
  .catch(async (error: unknown) => {
    console.error("PulseRoute development seed failed.");
    console.error(error);

    await prisma.$disconnect();
    await pool.end();

    process.exitCode = 1;
  });
