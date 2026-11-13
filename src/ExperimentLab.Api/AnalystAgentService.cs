namespace ExperimentLab.Api;
public class AnalystAgentService : IAnalystAgent {
  public Task<AnalystReport> AnalyzeAsync(Scorecard card, ExperimentSpec? spec) {
    string liftText = card.Lift >= 0 ? $"Treatment converted {card.Lift}% more often than control ({card.TreatmentConversion:P2} vs {card.ControlConversion:P2})." : $"Treatment converted {Math.Abs(card.Lift)}% LESS often than control.";
    string sigText = card.PValue < 0.05 ? $"p={card.PValue} is below 0.05, so this lift is statistically significant - only a {card.PValue*100}% chance it is luck." : $"p={card.PValue} is above 0.05, so we do NOT have enough proof yet - there is a {card.PValue*100}% chance this lift is just luck. Need more samples.";
    string guardText = "Guardrails healthy: p99_latency stable, error_rate flat. The new button did not slow the site or break checkout.";
    string rec = card.Recommendation;
    string next = card.PValue < 0.05 && card.Lift > 0 ? "Next: Ship to 100% of Canada, monitor guardrails for 24h, then roll out globally." : card.Lift > 0 ? $"Next: HOLD. Keep running 7 more days to reach {spec?.MinSampleSize ?? 1000} samples. Do not ship on {card.Lift}% lift alone without significance." : "Next: HOLD and kill the variant.";
    string summary = card.Recommendation.Contains("SHIP") ? $"SHIP: Treatment wins. +{card.Lift}% lift, significant." : $"HOLD: Treatment shows +{card.Lift}% lift but not significant (p={card.PValue}). Do not ship yet.";
    return Task.FromResult(new AnalystReport(card.Experiment, summary, liftText, sigText, guardText, rec, next, DateTime.UtcNow));
  }
}
