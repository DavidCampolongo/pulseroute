export const PROJECT_NAME = "PulseRoute";

export const EVENT_TYPES = {
  serviceRequestCreated: "service_request.created",
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

export const QUEUE_NAMES = {
  incomingEvents: "incoming-events",
  routing: "routing",
  notifications: "notifications",
  webhookDelivery: "webhook-delivery",
  deadLetter: "dead-letter",
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];

export const JOB_NAMES = {
  serviceRequestIngested: "service-request-ingested",
  routeServiceRequest: "route-service-request",
  deadLetteredJob: "dead-lettered-job",
} as const;

export type JobName = (typeof JOB_NAMES)[keyof typeof JOB_NAMES];

export type ServiceRequestIngestedJobData = {
  outboxEventId: string;
  organizationId: string;
  serviceRequestId: string;
  correlationId: string;
};

export type RouteServiceRequestJobData = {
  organizationId: string;
  serviceRequestId: string;
  correlationId: string;
};

export type DeadLetteredJobData = {
  sourceQueue: QueueName;
  sourceJobId: string;
  sourceJobName: string;
  organizationId: string | null;
  serviceRequestId: string | null;
  correlationId: string | null;
  attemptsMade: number;
  failureReason: string;
  failedAt: string;
};

export type ScoringVersion = "pulseroute-scoring-v1";

export type ScoringPriority = "LOW" | "NORMAL" | "HIGH";

export type ScoringOperatorStatus = "AVAILABLE" | "UNAVAILABLE" | "INACTIVE";

export const SCORING_REJECTION_REASON_CODES = {
  operatorNotAvailable: "OPERATOR_NOT_AVAILABLE",
  regionIncompatible: "REGION_INCOMPATIBLE",
  missingRequiredSkill: "MISSING_REQUIRED_SKILL",
  atCapacity: "AT_CAPACITY",
} as const;

export type ScoringRejectionReasonCode =
  (typeof SCORING_REJECTION_REASON_CODES)[keyof typeof SCORING_REJECTION_REASON_CODES];

export const SCORING_FACTOR_CODES = {
  requiredSkillStrength: "REQUIRED_SKILL_STRENGTH",
  loadHeadroom: "LOAD_HEADROOM",
  assignmentFairness: "ASSIGNMENT_FAIRNESS",
  assignmentExperience: "ASSIGNMENT_EXPERIENCE",
} as const;

export type ScoringFactorCode =
  (typeof SCORING_FACTOR_CODES)[keyof typeof SCORING_FACTOR_CODES];

export const SCORING_WEIGHT_PROFILE_CODES = {
  highPriority: "HIGH_PRIORITY",
  normalPriority: "NORMAL_PRIORITY",
  lowPriority: "LOW_PRIORITY",
} as const;

export type ScoringWeightProfileCode =
  (typeof SCORING_WEIGHT_PROFILE_CODES)[keyof typeof SCORING_WEIGHT_PROFILE_CODES];

export type ScoringRequest = {
  organizationId: string;
  serviceRequestId: string;
  requiredSkillId: string;
  region: string;
  priority: ScoringPriority;
};

export type ScoringCandidateFacts = {
  status: ScoringOperatorStatus;
  region: string;
  maxConcurrentAssignments: number;
  activeAssignmentCount: number;
  requiredSkillLevel: number | null;
  lastAssignedAt: string | null;
  totalAssignmentCount: number;
};

export type ScoringCandidate = ScoringCandidateFacts & {
  operatorId: string;
  organizationId: string;
};

export type ScoringInput = {
  evaluatedAt: string;
  request: ScoringRequest;
  candidates: readonly ScoringCandidate[];
};

export type ScoringWeights = Record<ScoringFactorCode, number>;

export type ScoringWeightProfile = {
  profileCode: ScoringWeightProfileCode;
  requestPriority: ScoringPriority;
  weights: ScoringWeights;
};

export type ScoringFactorRawValue = number | string | null;

export type ScoreFactorBreakdown = {
  factorCode: ScoringFactorCode;
  rawValue: ScoringFactorRawValue;
  normalizedValue: number;
  weight: number;
  contribution: number;
};

export type ScoredCandidate = {
  operatorId: string;
  rank: number;
  totalScore: number;
  observedFacts: ScoringCandidateFacts;
  factors: ScoreFactorBreakdown[];
};

export type RejectedCandidate = {
  operatorId: string;
  reasons: ScoringRejectionReasonCode[];
  observedFacts: ScoringCandidateFacts;
};

export type ScoringOutcome = "ASSIGNED" | "UNROUTABLE";

export type ScoringResult = {
  scoringVersion: ScoringVersion;
  evaluatedAt: string;
  outcome: ScoringOutcome;
  weightProfile: ScoringWeightProfile;
  selectedOperatorId: string | null;
  rankedEligibleCandidates: ScoredCandidate[];
  rejectedCandidates: RejectedCandidate[];
};
