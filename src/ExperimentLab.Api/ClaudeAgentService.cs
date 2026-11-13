using System.Text.Json;
using Confluent.Kafka;
namespace ExperimentLab.Api;
public class ClaudeAgentService : IAgentService {
  public async Task<ExperimentSpec> DesignExperimentAsync(string prompt) { await Task.Delay(300); return new ExperimentSpec("checkout-button-test","New checkout button increases conversion", new List<Variant>{ new("control",50), new("treatment",50)}, new Targeting("CA",10), "checkout_conversion", new List<string>{ "p99_latency","error_rate"}, 1000, 14); }
  public async Task<GovernanceResult> ValidateExperimentAsync(ExperimentSpec spec) { await Task.Delay(100); var v=new List<string>(); if(spec.GuardrailMetrics.Count<2) v.Add("Need 2 guardrails"); if(spec.Targeting.TrafficAllocation>10) v.Add("Capped at 10%"); return new GovernanceResult(v.Count==0, v, new List<string>{ "Add guardrails"}, "medium"); }
}
public class KafkaProducer : IKafkaProducer {
  private readonly string _bs; public KafkaProducer(IConfiguration c){ _bs=c["Kafka:BootstrapServers"]??"kafka:29092"; }
  public async Task PublishAsync(string topic, object payload){ var cfg=new ProducerConfig{ BootstrapServers=_bs, Acks=Acks.All }; using var p=new ProducerBuilder<string,string>(cfg).Build(); var r=await p.ProduceAsync(topic, new Message<string,string>{ Key=Guid.NewGuid().ToString(), Value=JsonSerializer.Serialize(payload)}); Console.WriteLine($"[Apache Kafka] {topic} offset={r.Offset}"); }
}
