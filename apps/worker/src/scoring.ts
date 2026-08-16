import {
  scoreRoutingCandidates,
  type ScoringInput,
  type ScoringResult,
} from "@pulseroute/scoring";

export const SCORING_FAULT_RATE = 0.1;

export class InjectedScoringFaultError extends Error {
  constructor() {
    super("Injected scoring failure");
    this.name = "InjectedScoringFaultError";
  }
}

export type RoutingScorer = (input: ScoringInput) => ScoringResult;

export type ScoringFaultDecision = (input: ScoringInput) => boolean;

export type CreateWorkerScorerOptions = {
  faultInjectionEnabled: boolean;
  shouldInjectFault?: ScoringFaultDecision;
  scorer?: RoutingScorer;
};

function defaultFaultDecision(): boolean {
  return Math.random() < SCORING_FAULT_RATE;
}

export function createWorkerScorer(
  options: CreateWorkerScorerOptions,
): RoutingScorer {
  const scorer = options.scorer ?? scoreRoutingCandidates;
  const shouldInjectFault = options.shouldInjectFault ?? defaultFaultDecision;

  return (input) => {
    if (options.faultInjectionEnabled && shouldInjectFault(input)) {
      throw new InjectedScoringFaultError();
    }

    return scorer(input);
  };
}
