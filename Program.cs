using System.Diagnostics.Metrics;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

var meter = new Meter("Measure.Rx.Buffering");
var produced = meter.CreateCounter<long>("produced_total");
var consumed = meter.CreateCounter<long>("consumed_total");
var producerLatency = meter.CreateHistogram<long>("producer_latency_ms");
var consumerLatency = meter.CreateHistogram<long>("consumer_latency_ms");

Console.WriteLine($"dotnet counters monitor --process-id {Environment.ProcessId} --counters {meter.Name}");

const int takeLast = 10;
using var tsc = new CancellationTokenSource();
using var subject = new Subject<Record>();
using var sub = subject
  .GroupBy(r => r.Key)
  .SelectMany(g => g
    .Buffer(TimeSpan.FromMilliseconds(500))
    .SelectMany(buffer => buffer.TakeLast(takeLast)))
  .Subscribe(record =>
  {
    consumed.Add(1);
    consumerLatency.Record(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - record.Timestamp);
  });

Task.Run(async () =>
{
  await foreach (var record in ProduceRecords(tsc.Token).WithCancellation(tsc.Token))
  {
    produced.Add(1);
    producerLatency.Record(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - record.Timestamp);

    subject.OnNext(record);
  }
}, tsc.Token);

Console.ReadLine();
tsc.Cancel();

static async IAsyncEnumerable<Record> ProduceRecords([EnumeratorCancellation] CancellationToken cancellationToken)
{
  var ch = Channel.CreateUnbounded<Record>(new UnboundedChannelOptions
  {
    SingleWriter = true,
    SingleReader = true
  });

  var timer = new Timer(_ =>
  {
    for (var i = 0; i < 300; i++)
    {
      ch.Writer.TryWrite(new Record());
    }
  }, null, 0, 100);

  cancellationToken.Register(() => timer.Dispose());

  await foreach (var record in ch.Reader.ReadAllAsync(cancellationToken).WithCancellation(cancellationToken))
  {
    yield return record;
  }
}

record Record
{
  private static Random Rnd = new();
  public int Key { get; } = Rnd.Next(500);
  public long Timestamp { get; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
