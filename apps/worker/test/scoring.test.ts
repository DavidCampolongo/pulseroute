import { scoreRoutingCandidates, type ScoringInput } from "@pulseroute/scoring";
import { describe, expect, it, vi } from "vitest";

import {
  createWorkerScorer,
  InjectedScoringFaultError,
  SCORING_FAULT_RATE,
} from "../src/scoring.js";
import { createScoringInput } from "../src/routing-workflow.js";

const scoringInput: ScoringInput = {
  evaluatedAt: "2026-08-15T12:00:00.000Z",
  request: {
    organizationId: "organization-1",
    serviceRequestId: "request-1",
    requiredSkillId: "skill-1",
    region: "WEST",
    priority: "NORMAL",
  },
  candidates: [
    {
      operatorId: "operator-1",
      organizationId: "organization-1",
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: 2,
      activeAssignmentCount: 0,
      requiredSkillLevel: 4,
      lastAssignedAt: null,
      totalAssignmentCount: 0,
    },
  ],
};

describe("createWorkerScorer", () => {
  it("uses the bounded production fault rate", () => {
    expect(SCORING_FAULT_RATE).toBe(0.1);
  });

  it("never consults the fault decision when fault injection is disabled", () => {
    const shouldInjectFault = vi.fn(() => true);
    const scorer = createWorkerScorer({
      faultInjectionEnabled: false,
      shouldInjectFault,
    });

    expect(scorer(scoringInput)).toEqual(scoreRoutingCandidates(scoringInput));
    expect(shouldInjectFault).not.toHaveBeenCalled();
  });

  it("throws a typed scoring fault when the injected decision forces a fault", () => {
    const scorerImplementation = vi.fn(scoreRoutingCandidates);
    const scorer = createWorkerScorer({
      faultInjectionEnabled: true,
      shouldInjectFault: () => true,
      scorer: scorerImplementation,
    });

    expect(() => scorer(scoringInput)).toThrow(InjectedScoringFaultError);
    expect(scorerImplementation).not.toHaveBeenCalled();
  });

  it("calls the pure scorer when the injected decision declines a fault", () => {
    const scorer = createWorkerScorer({
      faultInjectionEnabled: true,
      shouldInjectFault: () => false,
    });

    expect(scorer(scoringInput)).toEqual(scoreRoutingCandidates(scoringInput));
  });
});

describe("createScoringInput", () => {
  it("converts database rows into an explicit JSON-compatible scorer contract", () => {
    expect(
      createScoringInput(
        {
          id: "request-1",
          organizationId: "organization-1",
          status: "PENDING",
          requiredSkillId: "skill-1",
          priority: "HIGH",
          region: "WEST",
        },
        [
          {
            operatorId: "operator-1",
            organizationId: "organization-1",
            status: "AVAILABLE",
            region: "WEST",
            maxConcurrentAssignments: 3,
            activeAssignmentCount: 1,
            requiredSkillId: "skill-1",
            requiredSkillLevel: 5,
            hasRequiredSkill: true,
            lastAssignedAt: new Date("2026-08-14T12:00:00.000Z"),
            totalAssignmentCount: 7,
          },
        ],
        "2026-08-15T12:00:00.000Z",
      ),
    ).toEqual({
      evaluatedAt: "2026-08-15T12:00:00.000Z",
      request: {
        organizationId: "organization-1",
        serviceRequestId: "request-1",
        requiredSkillId: "skill-1",
        region: "WEST",
        priority: "HIGH",
      },
      candidates: [
        {
          operatorId: "operator-1",
          organizationId: "organization-1",
          status: "AVAILABLE",
          region: "WEST",
          maxConcurrentAssignments: 3,
          activeAssignmentCount: 1,
          requiredSkillLevel: 5,
          lastAssignedAt: "2026-08-14T12:00:00.000Z",
          totalAssignmentCount: 7,
        },
      ],
    });
  });
});
