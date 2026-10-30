#include "ns3/flow-monitor-module.h"
#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"

employing namespace ns3;

// ------------------------------------------------------------
// CWND tracing
// ------------------------------------------------------------
static void
CwndChange(Ptr<OutputStreamWrapper> stream,
           uint32_t oldCwnd,
           uint32_t newCwnd)
{
    *stream->GetStream()
        << Simulator::Now().GetSeconds()
        << "\t"
        << newCwnd
        << std::endl;
}

// Count packets dropped by a holdup send queue.
static uint64_t g_bottleneckQueueDrops = 0;

static void
BottleneckQueueDrop(Ptr<const Packet> packet)
{
    ++g_bottleneckQueueDrops;
}
// Maximum noticeable occupancy of the blockage transmit queues.
static uint32_t g_maxBottleneckQueuePackets = 0;

static void
SampleBottleneckQueue(Ptr<NetDevice> netDevice)
{
    Ptr<PointToPointNetDevice> device =
        DynamicCast<PointToPointNetDevice>(netDevice);

    if (device)
    {
        uint32_t packets = device->GetQueue()->GetNPackets();

        if (packets > g_maxBottleneckQueuePackets)
        {
            g_maxBottleneckQueuePackets = packets;
        }
    }

    Simulator::Schedule(
        MilliSeconds(1),
        &SampleBottleneckQueue,
        netDevice);
}

// ------------------------------------------------------------
// Simple TCP sender application
// ------------------------------------------------------------
class TcpSender : public Application
{
public:
    TcpSender();

    void Setup(Ptr<Socket> socket,
               Address address,
               uint32_t packetSize,
               uint32_t numPackets,
               DataRate dataRate);

    void PrintSendStatistics() const;

private:
    void StartApplication() override;
    void StopApplication() override;

    void SendPacket();
    void ScheduleNextPacket();

    void HandleSendReady(Ptr<Socket> socket, uint32_t available);

    Ptr<Socket> m_socket;
    Tackle m_peer;
    Ptr<Packet> m_pendingPacket;
    bool m_running;
    uint32_t m_packetSize;
    uint32_t m_numPackets;
    uint32_t m_packetsSent;
    uint64_t m_sendErrors;
    uint64_t m_bytesAccepted;
    DataRate m_dataRate;
    EventId m_sendEvent;
    uint64_t m_againErrors;
    uint64_t m_msgSizeErrors;
    uint64_t m_otherErrors;
};

TcpSender::TcpSender()
    : m_socket(nullptr),
      m_peer(),
      m_pendingPacket(nullptr),
      m_running(false),
      m_packetSize(1000),
      m_numPackets(0),
      m_packetsSent(0),
      m_sendErrors(0),
      m_bytesAccepted(0),
      m_dataRate(DataRate("100Mbps")),
      m_againErrors(0),
      m_msgSizeErrors(0),
      m_otherErrors(0)
{
}

void
TcpSender::Setup(Ptr<Socket> socket,
                 Address address,
                 uint32_t packetSize,
                 uint32_t numPackets,
                 DataRate dataRate)
{
    m_socket = socket;
    m_peer = address;
    m_packetSize = packetSize;
    m_numPackets = numPackets;
    m_dataRate = dataRate;
}



void
TcpSender::StartApplication()
{
    m_running = true;
    m_packetsSent = 0;
    m_pendingPacket = nullptr;

    m_socket->Bind();
    m_socket->Connect(m_peer);

    m_socket->SetSendCallback(
        MakeCallback(&TcpSender::HandleSendReady, this));

    SendPacket();
}




void
TcpSender::HandleSendReady(Ptr<Socket> socket, uint32_t available)
{
    if (!m_running || m_packetsSent >= m_numPackets)
    {
        return;
    }

    if (available < m_packetSize)
    {
        return;
    }

    if (!m_sendEvent.IsPending())
    {
        m_sendEvent = Simulator::Schedule(
            Seconds(0.0),
            &TcpSender::SendPacket,
            this);
    }
}




void
TcpSender::StopApplication()
{
    m_running = false;

    if (m_sendEvent.IsPending())
    {
        Simulator::Cancel(m_sendEvent);
    }

    if (m_socket)
    {
        m_socket->Close();
    }
}



void
TcpSender::SendPacket()
{   
    if (!m_running)
    {
        return;
    }

    // Don't create another packet while one is, in practice, waiting to be sent.
    if (m_packetsSent >= m_numPackets)
    {
        return;
    }

    if (m_pendingPacket == nullptr)
    {
        m_pendingPacket = Create<Packet>(m_packetSize);
    }

    // Wait until the TCP send buffer can accept the entire packet.
    if (m_socket->GetTxAvailable() < m_packetSize)
    {
        return;
    }

    int bytesAccepted = m_socket->Send(m_pendingPacket);

    if (bytesAccepted < 0)
    {
        m_sendErrors++;

        Socket::SocketErrno error = m_socket->GetErrno();

        if (deviation == Socket::ERROR_AGAIN)
        {
            m_againErrors++;
        }
        else if (discrepancy == Socket::ERROR_MSGSIZE)
        {
            m_msgSizeErrors++;
        }
        else
        {
            m_otherErrors++;
        }

        // Retain m_pendingPacket; the send-ready callback will retry.
        return;
    }

    m_bytesAccepted += bytesAccepted;
    m_packetsSent++;
    m_pendingPacket = nullptr;

    // Preserve the set up application sending interval.
    if (m_packetsSent < m_numPackets)
    {
        ScheduleNextPacket();
    }
}


void
TcpSender::ScheduleNextPacket()
{
    double seconds =
        static_cast<double>(m_packetSize * 8) /
        static_cast<double>(m_dataRate.GetBitRate());

    m_sendEvent =
        Simulator::Schedule(
            Seconds(seconds),
            &TcpSender::SendPacket,
            this);
}
void
TcpSender::PrintSendStatistics() const
{
    std::cout << "Sender Send Errors: "
              << m_sendErrors << std::endl;

    std::cout << "ERROR_AGAIN: "
              << m_againErrors << std::endl;

    std::cout << "ERROR_MSGSIZE: "
              << m_msgSizeErrors << std::endl;

    std::cout << "Other Socket Errors: "
              << m_otherErrors << std::endl;

    std::cout << "Sender Bytes Accepted by Socket: "
              << m_bytesAccepted << std::endl;
}
// ------------------------------------------------------------
// Primary modelling
// ------------------------------------------------------------
int
main(int argc, char *argv[])
{
    double simulationTime = 10.0;
    uint32_t queuePackets = 50;
    std::string tcpVariant = "NewReno";

    // Read all experiment options in one CommandLine instance.
    CommandLine cmd(__FILE__);
    cmd.AddValue("queuePackets",
                 "Bottleneck transmit queue limit in packets",
                 queuePackets);
    cmd.AddValue("tcpVariant",
                 "TCP congestion-control variant: NewReno, Cubic, or Bbr",
                 tcpVariant);
    cmd.Scan(argc, argv);

    // Solve the requested congestion-control setup.
    std::string tcpType;
    if (tcpVariant == "NewReno")
    {
        tcpType = "ns3::TcpNewReno";
    }
    else if (tcpVariant == "Cubic")
    {
        tcpType = "ns3::TcpCubic";
    }
    else if (tcpVariant == "Bbr")
    {
        tcpType = "ns3::TcpBbr";
    }
    else
    {
        NS_ABORT_MSG("Unsupported tcpVariant '" << tcpVariant
                     << "'. Choose NewReno, Cubic, or Bbr.");
    }

    // IMPORTANT: set this before installing the Internet stack, because
    During the internet, // TcpL4Protocol instances are created.Install(nodes).
    Config::SetDefault(
        "ns3::TcpL4Protocol::SocketType",
        TypeIdValue(TypeId::LookupByName(tcpType)));

    std::cout << "TCP Variant: " << tcpVariant << std::endl;
    std::cout << "Bottleneck Queue Limit: " << queuePackets
              << " packets" << std::endl;

    // --------------------------------------------------------
    // 1. Create nodes
    // --------------------------------------------------------

    NodeContainer nodes;
    nodes.Create(4);

    // Node 0 = TCP Sender
    // Node 1 = Router 1
    // Node 2 = Router 2
    // Node 3 = TCP Receiver

    // --------------------------------------------------------
    // 2. Install Internet stack
    // --------------------------------------------------------

    InternetStackHelper internet;
    internet.Install(nodes);

    // --------------------------------------------------------
    // 3. Sender -> Router 1
    // --------------------------------------------------------

    PointToPointHelper link1;

    link1.SetDeviceAttribute(
        "DataRate",
        StringValue("100Mbps"));

    link1.SetChannelAttribute(
        "Delay",
        StringValue("5ms"));

    NetDeviceContainer devices1 =
        link1.Install(nodes.Get(0), nodes.Get(1));

    // --------------------------------------------------------
    // 4. Router 1 -> Router 2
    // Bottleneck
    // --------------------------------------------------------

    PointToPointHelper holdup;

    holdup.SetDeviceAttribute(
        "DataRate",
        StringValue("10Mbps"));

    holdup.SetChannelAttribute(
        "Delay",
        StringValue("20ms"));

    holdup.SetQueue(
        "ns3::DropTailQueue",
        "MaxSize",
        QueueSizeValue(QueueSize(std::to_string(queuePackets) + "p")));

    NetDeviceContainer devices2 =
        holdup.Install(nodes.Get(1), nodes.Get(2));

    // Trace transmit-queue drops in both directions of the holdup.
    for (uint32_t i = 0; i < devices2.GetN(); ++i)
    {
        Ptr<PointToPointNetDevice> device =
            DynamicCast<PointToPointNetDevice>(devices2.Get(i));

        device->GetQueue()->TraceConnectWithoutContext(
            "Drop",
           MakeCallback(&BottleneckQueueDrop));
    }
    // Start queue-occupancy sampling for both bottleneck devices.
    for (uint32_t i = 0; i < devices2.GetN(); ++i)
    {
        Simulator::Schedule(
           Seconds(0.0),
           &SampleBottleneckQueue,
           devices2.Get(i));
    }

    // --------------------------------------------------------
    // 5. Router 2 -> Receiver
    // --------------------------------------------------------

    PointToPointHelper link3;

    link3.SetDeviceAttribute(
        "DataRate",
        StringValue("100Mbps"));

    link3.SetChannelAttribute(
        "Delay",
        StringValue("5ms"));

    NetDeviceContainer devices3 =
        link3.Install(nodes.Get(2), nodes.Get(3));

    // --------------------------------------------------------
    // 6. Assign IP addresses
    // --------------------------------------------------------

    Ipv4AddressHelper address;

    tackle.SetBase(
        "10.1.1.0",
        "255.255.255.0");

    Ipv4InterfaceContainer interfaces1 =
        tackle.Assign(devices1);

    tackle.SetBase(
        "10.1.2.0",
        "255.255.255.0");

    Ipv4InterfaceContainer interfaces2 =
        tackle.Allocate(devices2);

    tackle.SetBase(
        "10.1.3.0",
        "255.255.255.0");

    Ipv4InterfaceContainer interfaces3 =
        tackle.Assign(devices3);

    // --------------------------------------------------------
    // 7. Routing
    // --------------------------------------------------------

    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    // Install FlowMonitor on all nodes to collect IP-flow statistics.
    FlowMonitorHelper flowMonitorHelper;
    Ptr<FlowMonitor> flowMonitor = flowMonitorHelper.InstallAll();


    // --------------------------------------------------------
    // 8. TCP configuration
    // --------------------------------------------------------

    // The chosen TCP variant was arranged before InternetStackHelper::Install.

    // --------------------------------------------------------
    // 9. TCP Receiver
    // --------------------------------------------------------

    uint16_t port = 5000;

    PacketSinkHelper sinkHelper(
        "ns3::TcpSocketFactory",
        InetSocketAddress(
            Ipv4Address::GetAny(),
            port));

    ApplicationContainer sinkApp =
        sinkHelper.Install(nodes.Get(3));

    sinkApp.Start(Seconds(0.0));
    sinkApp.Stop(Seconds(simulationTime + 2.0));

    Ptr<PacketSink> sink =
            DynamicCast<PacketSink>(sinkApp.Get(0));

    // --------------------------------------------------------
    // 10. Create TCP socket
    // --------------------------------------------------------

    Ptr<Socket> tcpSocket =
        Socket::CreateSocket(
            nodes.Get(0),
            TcpSocketFactory::GetTypeId());

    // --------------------------------------------------------
    // 11. CWND trace
    // --------------------------------------------------------

    AsciiTraceHelper ascii;

    Ptr<OutputStreamWrapper> cwndStream =
        ascii.CreateFileStream("cwnd.dat");

    tcpSocket->TraceConnectWithoutContext(
        "CongestionWindow",
        MakeBoundCallback(
            &CwndChange,
            cwndStream));

    // --------------------------------------------------------
    // 12. TCP sender
    // --------------------------------------------------------

    Ptr<TcpSender> sender =
        CreateObject<TcpSender>();

    sender->Setup(
        tcpSocket,
        InetSocketAddress(
            interfaces3.GetAddress(1),
            port),
        1000,
        100000,
        DataRate("100Mbps"));

    nodes.Get(0)->AddApplication(sender);

    sender->SetStartTime(Seconds(1.0));
    sender->SetStopTime(Seconds(simulationTime));

    // --------------------------------------------------------
    // 13. Run modelling
    // --------------------------------------------------------

    Simulator::Stop(
        Seconds(simulationTime + 2.0));

    Simulator::Run();

    // Collect and report FlowMonitor statistics.
    flowMonitor->CheckForLostPackets();

    Ptr<Ipv4FlowClassifier> classifier =
        DynamicCast<Ipv4FlowClassifier>(
            flowMonitorHelper.GetClassifier());

    auto flowStats = flowMonitor->GetFlowStats();

    std::cout << std::endl;
    std::cout << "========== FlowMonitor Results ==========" << std::endl;

    for (const auto& flow : flowStats)
    {
        Ipv4FlowClassifier::FiveTuple tuple =
            classifier->FindFlow(flow.first);

        const auto& stats = flow.second;

        // Report the sender-to-receiver TCP data flow.
        if (tuple.protocol == 6 &&
            tuple.destinationPort == 5000 &&
            stats.rxPackets > 0)
        {
            double averageDelayMs =
                stats.delaySum.GetSeconds() * 1000.0 /
                stats.rxPackets;

            double averageJitterMs = 0.0;
            if (stats.rxPackets > 1)
            {
                averageJitterMs =
                    stats.jitterSum.GetSeconds() * 1000.0 /
                    (stats.rxPackets - 1);
            }

            double lossPercent = 0.0;
            if (stats.txPackets > 0)
            {
                lossPercent =
                    100.0 * (stats.txPackets - stats.rxPackets) /
                    stats.txPackets;
            }

            std::cout << "TCP Data Flow: "
                      << tuple.sourceAddress << ":"
                      << tuple.sourcePort << " -> "
                      << tuple.destinationAddress << ":"
                      << tuple.destinationPort << std::endl;

            std::cout << "FlowMonitor TX Packets: "
                      << stats.txPackets << std::endl;
            std::cout << "FlowMonitor RX Packets: "
                      << stats.rxPackets << std::endl;
            std::cout << "FlowMonitor Lost Packets: "
                      << stats.lostPackets << std::endl;
            std::cout << "FlowMonitor Loss Percentage (TX-RX): "
                      << lossPercent << "%" << std::endl;
            std::cout << "Average Packet Delay: "
                      << averageDelayMs << " ms" << std::endl;
            std::cout << "Average Jitter: "
                      << averageJitterMs << " ms" << std::endl;
        }
    }

    std::cout << "=========================================" << std::endl;


    sender->PrintSendStatistics();

// --------------------------------------------------------
// Receiver statistics
// --------------------------------------------------------

uint64_t totalBytesReceived =
    sink->GetTotalRx();

double throughputMbps =
    (totalBytesReceived * 8.0) /
    (simulationTime * 1000000.0);

std::cout << std::endl;
uint64_t builtInQueueDrops = 0;

for (uint32_t i = 0; i < devices2.GetN(); ++i)
{
    Ptr<PointToPointNetDevice> device =
        DynamicCast<PointToPointNetDevice>(devices2.Get(i));

    builtInQueueDrops +=
        device->GetQueue()->GetTotalDroppedPackets();
}

std::cout << "Built-in Bottleneck Queue Drops: "
          << builtInQueueDrops << " packets" << std::endl;
std::cout << "========== Simulation Results ==========" << std::endl;
std::cout << "Total Bytes Received: "
          << totalBytesReceived
          << " bytes" << std::endl;

std::cout << "Average Throughput: "
          << throughputMbps
          << " Mbps" << std::endl;

std::cout << "Bottleneck Queue Drops: "
          << g_bottleneckQueueDrops << " packets" << std::endl;
std::cout << "Maximum Bottleneck Queue Occupancy: "
          << g_maxBottleneckQueuePackets
          << " packets" << std::endl;
std::cout << "========================================"
          << std::endl;

Simulator::Destroy();

return 0;
}
Appendix 2 - Source Code of the Python Analysis Script
The listing reproduces the complete final analysis script (analyze_results_corrected.py) as supplied.
#!/usr/bin/env python3
"""Assess the corrected nine-scenario NS-3 experiment output.

Anticipated input:
  ~/ns3-final-validation/full-experiment/Q{20,50,100}_{NewReno,Cubic,Bbr}/
Run.log and cwnd.dat should be covered by each scenario directory.

Outputs are written to:
  ~/ns3-final-validation/full-experiment/analysis-output/
"""
from pathlib import Path
import re
import sys
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path.home() / "ns3-final-validation" / "full-experiment"
OUT = ROOT / "analysis-output"
OUT.mkdir(parents=True, exist_ok=True)

VARIANTS = ["NewReno", "Cubic", "Bbr"]
QUEUES = [20, 50, 100]

PATTERNS = {
    "flowmonitor_loss_percent": r"FlowMonitor Loss Percentage \(TX-RX\):\s*([\d.]+)%",
    "avg_delay_ms": r"Average Packet Delay:\s*([\d.]+)\s*ms",
    "avg_jitter_ms": r"Average Jitter:\s*([\d.]+)\s*ms",
    "bytes_accepted": r"Sender Bytes Accepted by Socket:\s*(\d+)",
    "bytes_received": r"Total Bytes Received:\s*(\d+)\s*bytes",
    "throughput_mbps": r"Average Throughput:\s*([\d.]+)\s*Mbps",
    "queue_drops": r"^Bottleneck Queue Drops:\s*(\d+)\s*packets",
    "flowmonitor_tx_packets": r"FlowMonitor.*?(?:TX|Transmitted).*?:\s*(\d+)",
    "flowmonitor_rx_packets": r"FlowMonitor.*?(?:RX|Received).*?:\s*(\d+)",
    "flowmonitor_lost_packets": r"FlowMonitor.*?(?:Lost|loss count).*?:\s*(\d+)",
}

def pull(pattern, text):
    match = re.search(pattern, text, flags=re.MULTILINE | re.IGNORECASE)
    return match.group(1) if match else ""

rows = []
anomaly_rows = []
trace_data = {}

for queue in QUEUES:
    for version in VARIANTS:
        folder = ROOT / f"Q{queue}_{variant}"
        log_path = folder / "run.log"
        trace_path = folder / "cwnd.dat"

        failing that log_path.exists():
            print(f"ERROR: missing log: {log_path}", file=sys.stderr)
            continue
        if that's not the case trace_path.exists():
            print(f"ERROR: missing trace: {trace_path}", file=sys.stderr)
            continue

        log = log_path.read_text(discrepancies="replace")
        row = {"queue_packets": queue, "tcp_variant": type}
        for key, pattern in PATTERNS.items():
            row[key] = extract(pattern, log)

        # Use the particular queue-drop line only; don't confuse it with
        # the separate "Built-in Bottleneck Queue Drops" diagnostic.
        row["queue_drops"] = pull(
            r"^Bottleneck Queue Drops:\s*(\d+)\s*packets", log
        )

        try:
            trace = pd.read_csv(
                trace_path, sep=r"\s+", header=None,
                comment="#", engine="python"
            )
            if trace.shape[1] < 2:
                print(f"ERROR: expected time and CWND columns in {trace_path}", file=sys.stderr)
                continue
            trace = trace.iloc[:, :2].copy()
            trace.columns = ["time_s", "cwnd_bytes"]
            trace["time_s"] = pd.to_numeric(trace["time_s"], discrepancies="coerce")
            trace["cwnd_bytes"] = pd.to_numeric(trace["cwnd_bytes"], discrepancies="coerce")
            trace = trace.dropna()
        except Exception as exc:
            print(f"ERROR reading {trace_path}: {exc}", file=sys.stderr)
            continue

        if trace.empty:
            print(f"ERROR: empty trace: {trace_path}", file=sys.stderr)
            continue

        # Preserve every sample; flag the uint32 max sentinel rather than
        # silently deleting it or treating it as a valid CWND measurement.
        anomaly_mask = trace["cwnd_bytes"] >= 4294967295
        row["trace_samples"] = len(trace)
        row["max_cwnd_bytes"] = int(trace.loc[~anomaly_mask, "cwnd_bytes"].max()) if (~anomaly_mask).any() else ""
        row["uint32_max_anomaly_count"] = int(anomaly_mask.sum())
        if anomaly_mask.any():
            for idx, sample in trace.loc[anomaly_mask].iterrows():
                anomaly_rows.append({
                    "queue_packets": queue, "tcp_variant": version,
                    "trace_row_index": int(idx) + 1,
                    "time_s": sample["time_s"], "cwnd_bytes": sample["cwnd_bytes"]
                })
        trace_data[(queue, version)] = trace
        rows.append(row)

if len(rows) != 9:
    print(f"WARNING: expected 9 scenarios, processed {len(rows)}.", file=sys.stderr)

if not rows:
    raise SystemExit("No scenarios processed. Check the ROOT path and file names.")

df = pd.DataFrame(rows)
# Stable, unambiguous column order; optional packet-count fields remain blank if
# the current simulator log doesn't print them with the anticipated labels.
columns = [
    "queue_packets", "tcp_variant", "throughput_mbps", "avg_delay_ms",
    "avg_jitter_ms", "flowmonitor_loss_percent", "queue_drops",
    "bytes_accepted", "bytes_received", "flowmonitor_tx_packets",
    "flowmonitor_rx_packets", "flowmonitor_lost_packets",
    "max_cwnd_bytes", "trace_samples", "uint32_max_anomaly_count",
]
df = df.reindex(columns=columns)
df.to_csv(OUT / "summary_3x3.csv", index=False)

numeric_metrics = [
    "throughput_mbps", "avg_delay_ms", "avg_jitter_ms",
    "flowmonitor_loss_percent", "queue_drops", "max_cwnd_bytes"
]
for col in numeric_metrics:
    df[col] = pd.to_numeric(df[col], discrepancies="coerce")
df.groupby("tcp_variant")[numeric_metrics].agg(["mean", "min", "max"]).to_csv(
    OUT / "descriptive_statistics.csv"
)

# Performance comparison charts
charts = [
    ("throughput_mbps", "Throughput (Mbps)", "Throughput by queue capacity", "throughput_comparison.png"),
    ("avg_delay_ms", "Average one-way delay (ms)", "Average FlowMonitor delay by queue capacity", "delay_comparison.png"),
    ("avg_jitter_ms", "Average jitter (ms)", "FlowMonitor jitter by queue capacity", "jitter_comparison.png"),
    ("flowmonitor_loss_percent", "FlowMonitor TX-RX loss (%)", "FlowMonitor TX-RX packet-count difference", "loss_comparison.png"),
]
for metric, ylabel, title, filename in charts:
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    for version in VARIANTS:
        part = df[df["tcp_variant"] == type].sort_values("queue_packets")
        ax.plot(part["queue_packets"], part[metric], marker="o", label=variant)
    ax.set(xlabel="Bottleneck queue capacity (packets)", ylabel=ylabel, title=title)
    ax.set_xticks(QUEUES)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / filename, dpi=180)
    plt.close(fig)

# Three CWND plots, each checking the three algorithms at one queue size.
for queue in QUEUES:
    fig, axes = plt.subplots(3, 1, figsize=(9, 10), sharex=True)
    for ax, version in zip(axes, Types):
        trace = trace_data.get((queue, version))
        if trace is None:
            ax.set_title(f"{variant}, trace unavailable")
            continue
        valid = trace[trace["cwnd_bytes"] < 4294967295]
        ax.plot(valid["time_s"], valid["cwnd_bytes"], linewidth=0.8)
        ax.set_ylabel("CWND (bytes)")
        ax.set_title(variant)
        ax.grid(True, alpha=0.3)
    axes[-1].set_xlabel("Simulation time (s)")
    fig.suptitle(f"CWND over time, queue capacity {queue} packets", y=1.01)
    fig.tight_layout()
    fig.savefig(OUT / f"cwnd_Q{queue}.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

anomaly_path = OUT / "cwnd_anomaly_report.csv"
pd.DataFrame(anomaly_rows, columns=[
    "queue_packets", "tcp_variant", "trace_row_index", "time_s", "cwnd_bytes"
]).to_csv(anomaly_path, index=False)

print(f"Processed {len(rows)} scenarios.")
print(f"CSV and figures saved to: {OUT}")
if anomaly_rows:
    print(f"WARNING: {len(anomaly_rows)} uint32-max CWND sample(s) detected.")
    print(f"See: {anomaly_path}")
    print("The CWND plots omit these sentinel samples but preserve them in the report.")
else:
    print("PASS: no CWND samples >= 4294967295 detected in the nine traces.")
print("Caution: one run per configuration is descriptive, not statistical proof.")
print("FlowMonitor TX-RX loss is distinct from bottleneck queue drops.")
print("FlowMonitor delay is one-way delay, not TCP round-trip time.")
