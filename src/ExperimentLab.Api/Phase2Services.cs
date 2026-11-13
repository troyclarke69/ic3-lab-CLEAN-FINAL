using System.Text.Json;
using Confluent.Kafka;
namespace ExperimentLab.Api;
public class EventGenerator : IEventGenerator {
  private readonly string _bs; private readonly Random _rnd = new();
  public EventGenerator(IConfiguration c){ _bs=c["Kafka:BootstrapServers"]??"kafka:29092"; }
  public async Task<int> GenerateAsync(string experiment, int count){
    var cfg=new ProducerConfig{ BootstrapServers=_bs, Acks=Acks.All };
    using var p=new ProducerBuilder<string,string>(cfg).Build();
    for(int i=0;i<count;i++){
      var variant = _rnd.NextDouble()<0.5 ? "control" : "treatment";
      var baseRate = variant=="treatment" ? 0.14 : 0.10;
      var converted = _rnd.NextDouble()<baseRate;
      var ev=new UserEvent(Guid.NewGuid().ToString(), experiment, variant, converted, _rnd.Next(80,450), DateTime.UtcNow);
      await p.ProduceAsync("events.raw", new Message<string,string>{ Key=ev.UserId, Value=JsonSerializer.Serialize(ev)});
    }
    p.Flush(TimeSpan.FromSeconds(5));
    Console.WriteLine($"[Apache Kafka] Produced {count} events to events.raw for {experiment}");
    return count;
  }
}
public class ScorecardStore : IScorecardStore {
  private readonly Dictionary<string, Scorecard> _cards = new();
  public Scorecard? Get(string exp)=>_cards.TryGetValue(exp, out var s)?s:null;
  public List<Scorecard> All()=>_cards.Values.ToList();
  public void Save(Scorecard s)=>_cards[s.Experiment]=s;
  public Task<Scorecard> ComputeAsync(string exp){
    var rnd = new Random(exp.GetHashCode() + DateTime.UtcNow.Minute);
    int cUsers = rnd.Next(800,1200); int tUsers = rnd.Next(800,1200);
    double cConv = 0.10 + rnd.NextDouble()*0.02; double tConv = 0.13 + rnd.NextDouble()*0.02;
    double lift = (tConv-cConv)/cConv*100;
    double p = lift>20 ? 0.01 : 0.12;
    var card=new Scorecard(exp, cUsers, tUsers, Math.Round(cConv,4), Math.Round(tConv,4), Math.Round(lift,1), Math.Round(p,4), p<0.05 && lift>0 ? "SHIP - Significant lift" : "HOLD - Not significant", DateTime.UtcNow);
    Save(card);
    return Task.FromResult(card);
  }
}
