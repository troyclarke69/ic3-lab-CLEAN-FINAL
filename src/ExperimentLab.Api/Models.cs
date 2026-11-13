namespace ExperimentLab.Api;
public record CreateExperimentRequest(string Prompt);
public record Variant(string Name, int Traffic);
public record Targeting(string? Country, int TrafficAllocation);
public record ExperimentSpec(string Name, string Hypothesis, List<Variant> Variants, Targeting Targeting, string PrimaryMetric, List<string> GuardrailMetrics, int MinSampleSize, int DurationDays);
public record GovernanceResult(bool Approved, List<string> Violations, List<string> SuggestedFixes, string RiskLevel);
public interface IExperimentStore { void Save(ExperimentSpec s); List<ExperimentSpec> All(); }
public class InMemoryExperimentStore : IExperimentStore { private readonly List<ExperimentSpec> _items = new(); public void Save(ExperimentSpec s) => _items.Add(s); public List<ExperimentSpec> All() => _items; }
public interface IAgentService { Task<ExperimentSpec> DesignExperimentAsync(string prompt); Task<GovernanceResult> ValidateExperimentAsync(ExperimentSpec spec); }
public interface IKafkaProducer { Task PublishAsync(string topic, object payload); }
public record GenerateEventsRequest(string ExperimentName, int Count);
public record UserEvent(string UserId, string Experiment, string Variant, bool Converted, int LatencyMs, DateTime Timestamp);
public record Scorecard(string Experiment, int ControlUsers, int TreatmentUsers, double ControlConversion, double TreatmentConversion, double Lift, double PValue, string Recommendation, DateTime ComputedAt);
public interface IEventGenerator { Task<int> GenerateAsync(string experiment, int count); }
public interface IScorecardStore { Task<Scorecard> ComputeAsync(string experiment); Scorecard? Get(string experiment); List<Scorecard> All(); void Save(Scorecard s); }
public record AnalystReport(string Experiment, string Summary, string LiftInterpretation, string SignificanceInterpretation, string GuardrailInterpretation, string Recommendation, string NextSteps, DateTime GeneratedAt);
public interface IAnalystAgent { Task<AnalystReport> AnalyzeAsync(Scorecard card, ExperimentSpec? spec); }
