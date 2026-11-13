using Confluent.Kafka;
namespace ExperimentLab.Api;
public class KafkaEventConsumer : BackgroundService {
  private readonly ILogger<KafkaEventConsumer> _logger; public KafkaEventConsumer(ILogger<KafkaEventConsumer> logger)=>_logger=logger;
  protected override async Task ExecuteAsync(CancellationToken ct){ await Task.Delay(TimeSpan.FromSeconds(5), ct); var cfg=new ConsumerConfig{ BootstrapServers="kafka:29092", GroupId="experiment-aggregator", AutoOffsetReset=AutoOffsetReset.Earliest, EnableAutoCommit=false }; using var c=new ConsumerBuilder<string,string>(cfg).Build(); try{ c.Subscribe("events.raw"); }catch(Exception ex){ _logger.LogWarning(ex,"Subscribe failed"); return; } while(!ct.IsCancellationRequested){ try{ var r=c.Consume(ct); _logger.LogInformation("[Apache Kafka] offset={Offset}", r.Offset); c.Commit(r);}catch(OperationCanceledException){ break; }catch(Exception ex){ _logger.LogWarning(ex,"Consume error"); await Task.Delay(2000, ct); } } c.Close(); }
}
