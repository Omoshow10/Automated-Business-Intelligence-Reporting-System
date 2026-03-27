"""
generate_dashboard_screenshots.py
-----------------------------------
Generates Power BI-style dashboard PNG screenshots for the
Automated Business Intelligence Reporting System.

Output files (plain descriptive names — no figure numbers):
  outputs/executive_overview_dashboard.png
  outputs/pipeline_run_log_monitoring.png
  outputs/automated_management_report.png
  outputs/data_quality_validation_report.png
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import numpy as np
import os

os.makedirs("outputs", exist_ok=True)

# ── Power BI colour palette ───────────────────────────────────────────────────
NAVY    = "#1F3864"
BLUE    = "#2E75B6"
LBLUE   = "#BDD7EE"
TEAL    = "#00B0A0"
GREEN   = "#375623"
LGREEN  = "#E2EFDA"
ORANGE  = "#C55A11"
LORANGE = "#FCE4D6"
RED     = "#C00000"
LRED    = "#FFDEDE"
GREY    = "#595959"
LGREY   = "#F2F2F2"
WHITE   = "#FFFFFF"
DARK    = "#262626"
GOLD    = "#FFD966"
SILVER  = "#D9D9D9"

plt.rcParams.update({
    "font.family":       "DejaVu Sans",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "figure.facecolor":  LGREY,
})

# ── Helper: draw a KPI card ───────────────────────────────────────────────────
def kpi_card(fig, rect, label, value, delta, delta_good, accent):
    bg = LGREEN if delta_good else LORANGE
    ax = fig.add_axes(rect)
    ax.set_facecolor(bg)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")
    ax.add_patch(FancyBboxPatch((0,0),1,1,
                 boxstyle="round,pad=0.02",lw=2,edgecolor=accent,facecolor=bg))
    ax.add_patch(plt.Rectangle((0,0.82),1,0.18,color=accent,zorder=2))
    ax.text(0.5,0.91,label,ha="center",va="center",
            fontsize=8.5,fontweight="bold",color=WHITE,zorder=3)
    ax.text(0.5,0.50,value,ha="center",va="center",
            fontsize=21,fontweight="bold",color=accent)
    dc = GREEN if delta_good else ORANGE
    ax.text(0.5,0.22,delta,ha="center",va="center",
            fontsize=8.5,color=dc,fontstyle="italic",fontweight="bold")

# ── Helper: styled table ──────────────────────────────────────────────────────
def draw_table(ax, cols, rows, col_widths, x0=0.0, y0=1.0,
               row_h=0.09, header_h=0.10, font=8):
    ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off")
    col_x = [x0]
    for w in col_widths[:-1]:
        col_x.append(col_x[-1] + w)

    # header
    for cx, cw, ch in zip(col_x, col_widths, cols):
        ax.add_patch(FancyBboxPatch((cx, y0-header_h), cw-0.006, header_h-0.005,
                     boxstyle="round,pad=0.004",facecolor=NAVY,edgecolor=WHITE,lw=0.5))
        ax.text(cx+(cw-0.006)/2, y0-header_h/2, ch,
                ha="center",va="center",fontsize=font,fontweight="bold",color=WHITE)

    for r, row in enumerate(rows):
        ry  = y0 - header_h - (r+1)*row_h
        alt = LGREY if r % 2 == 0 else WHITE
        fail = any("Failed" in str(c) or "✘" in str(c) for c in row)
        rbg  = LRED if fail else alt
        for cx, cw, cell in zip(col_x, col_widths, row):
            ax.add_patch(FancyBboxPatch((cx, ry), cw-0.006, row_h-0.008,
                         boxstyle="round,pad=0.002",facecolor=rbg,edgecolor=SILVER,lw=0.3))
            cc = RED   if ("Failed" in str(cell) or "✘" in str(cell)) else \
                 GREEN if ("Completed" in str(cell) or "✔" in str(cell) or "PASS" in str(cell)) else \
                 BLUE  if "FIXED" in str(cell) else \
                 ORANGE if "WARN" in str(cell) else DARK
            fw = "bold" if cc in (GREEN, RED, BLUE, ORANGE) else "normal"
            ax.text(cx+(cw-0.006)/2, ry+row_h/2-0.004, str(cell),
                    ha="center",va="center",fontsize=font,color=cc,fontweight=fw)

# =============================================================================
# DASHBOARD 1 — Automated BI Dashboard: Executive Overview
# Matches: "Main dashboard showing automated KPI summary, data freshness
#  timestamp, pipeline status indicator, and key operational metrics."
# =============================================================================

fig = plt.figure(figsize=(22,14), facecolor=LGREY)

# ── Top header ────────────────────────────────────────────────────────────────
hdr = fig.add_axes([0, 0.935, 1, 0.065])
hdr.set_facecolor(NAVY); hdr.axis("off")
hdr.add_patch(plt.Rectangle((0,0),0.006,1,color=GOLD))
hdr.text(0.012, 0.65,
         "Automated Business Intelligence Reporting System",
         color=WHITE, fontsize=15, fontweight="bold", va="center")
hdr.text(0.012, 0.25,
         "Executive Overview Dashboard  ·  Power BI  ·  Reporting Period: Jan – Dec 2024",
         color=LBLUE, fontsize=9, va="center")
hdr.text(0.99, 0.70, "Data Freshness:  2024-12-31  06:32",
         color=GOLD, fontsize=9, va="center", ha="right")
hdr.text(0.99, 0.30, "Pipeline Status:  ✔ Completed  |  Run #96  |  10,800 records",
         color=LGREEN, fontsize=9, va="center", ha="right")

# ── KPI cards ─────────────────────────────────────────────────────────────────
cards = [
    ("Total Revenue",    "$28.4M",  "▲ +18.2% YoY", True,  NAVY),
    ("Gross Profit",     "$9.7M",   "▲ +21.4% YoY", True,  BLUE),
    ("Profit Margin",    "34.2%",   "▲ +3.3pp YoY", True,  TEAL),
    ("YoY Growth",       "18.2%",   "▲ vs 12.0% prior", True, GREEN),
    ("Transactions",     "10,800",  "▲ +15.3% YoY", True,  NAVY),
]
for i,(lbl,val,dlt,good,acc) in enumerate(cards):
    kpi_card(fig,[0.01+i*0.197,0.775,0.185,0.135],lbl,val,dlt,good,acc)

# ── Revenue trend ─────────────────────────────────────────────────────────────
months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
r22 = [1.60,1.55,1.75,1.70,1.80,1.85,1.90,1.95,2.00,2.05,2.30,2.55]
r23 = [1.80,1.75,1.95,1.92,2.05,2.10,2.18,2.22,2.35,2.42,2.65,2.90]
r24 = [2.15,2.10,2.30,2.28,2.45,2.52,2.60,2.65,2.80,2.88,3.10,3.42]

ax_tr = fig.add_axes([0.01, 0.415, 0.57, 0.325])
ax_tr.set_facecolor(WHITE)
ax_tr.plot(months, r22, color=LBLUE, lw=2,   marker="o", ms=4, label="2022")
ax_tr.plot(months, r23, color=BLUE,  lw=2.5, marker="o", ms=4, label="2023")
ax_tr.plot(months, r24, color=NAVY,  lw=3,   marker="o", ms=5, label="2024")
ax_tr.fill_between(range(12), r24, alpha=0.07, color=NAVY)
ax_tr.set_title("Monthly Revenue Trend  (USD Millions)",
                fontsize=11, fontweight="bold", color=NAVY, pad=8)
ax_tr.set_xticks(range(12)); ax_tr.set_xticklabels(months, fontsize=8)
ax_tr.set_ylabel("Revenue ($M)", fontsize=9)
ax_tr.legend(fontsize=9, loc="upper left", framealpha=0.7)
ax_tr.yaxis.grid(True, alpha=0.25); ax_tr.set_axisbelow(True)

# ── Customer segment donut ────────────────────────────────────────────────────
ax_seg = fig.add_axes([0.60, 0.415, 0.185, 0.325])
ax_seg.set_facecolor(WHITE)
seg_v = [42, 35, 23]
seg_c = [NAVY, BLUE, LBLUE]
wedges,_ = ax_seg.pie(seg_v, colors=seg_c, startangle=90,
                       wedgeprops=dict(width=0.52, edgecolor=WHITE, lw=2))
for w, lbl in zip(wedges, ["Enterprise\n42%","SMB\n35%","Consumer\n23%"]):
    ang = (w.theta2+w.theta1)/2
    x = 0.68*np.cos(np.radians(ang)); y = 0.68*np.sin(np.radians(ang))
    ax_seg.text(x,y,lbl,ha="center",va="center",fontsize=8,color=WHITE,fontweight="bold")
ax_seg.set_title("Revenue by\nCustomer Segment",
                 fontsize=10, fontweight="bold", color=NAVY, pad=6)

# ── Channel revenue bars ──────────────────────────────────────────────────────
ax_ch = fig.add_axes([0.795, 0.415, 0.195, 0.325])
ax_ch.set_facecolor(WHITE)
chs = ["Direct","Partner","Online"]
chv = [12.8, 9.9, 5.7]
bars = ax_ch.barh(chs, chv, color=[NAVY,BLUE,TEAL], height=0.5, edgecolor=WHITE)
for b,v in zip(bars,chv):
    ax_ch.text(v+0.2, b.get_y()+b.get_height()/2,
               f"${v}M", va="center", fontsize=9, color=DARK, fontweight="bold")
ax_ch.set_title("Revenue by Channel ($M)",
                fontsize=10, fontweight="bold", color=NAVY, pad=6)
ax_ch.set_xlim(0,16); ax_ch.xaxis.grid(True, alpha=0.25)

# ── Regional bar chart ────────────────────────────────────────────────────────
ax_rg = fig.add_axes([0.01, 0.060, 0.38, 0.310])
ax_rg.set_facecolor(WHITE)
regs = ["East","North","West","South","International"]
regv = [7.2, 6.4, 5.9, 5.3, 3.6]
bc   = [NAVY if v==max(regv) else BLUE for v in regv]
bs   = ax_rg.bar(regs, regv, color=bc, width=0.58, edgecolor=WHITE)
for b,v in zip(bs,regv):
    ax_rg.text(b.get_x()+b.get_width()/2, v+0.08,
               f"${v}M", ha="center", fontsize=9, fontweight="bold", color=DARK)
ax_rg.set_title("Revenue by Region ($M)  —  2024",
                fontsize=10, fontweight="bold", color=NAVY, pad=6)
ax_rg.set_ylim(0,9); ax_rg.yaxis.grid(True, alpha=0.25); ax_rg.set_axisbelow(True)

# ── Profit margin quarterly trend ─────────────────────────────────────────────
ax_pm = fig.add_axes([0.42, 0.060, 0.29, 0.310])
ax_pm.set_facecolor(WHITE)
qtrs  = ["Q1'22","Q2'22","Q3'22","Q4'22","Q1'23","Q2'23",
         "Q3'23","Q4'23","Q1'24","Q2'24","Q3'24","Q4'24"]
margs = [30.2,31.5,32.1,33.4,32.8,33.2,33.9,34.5,33.5,34.0,34.8,35.2]
ax_pm.fill_between(range(12), margs, 29, alpha=0.12, color=TEAL)
ax_pm.plot(range(12), margs, color=TEAL, lw=2.5, marker="s", ms=5)
ax_pm.axhline(34.2, color=ORANGE, ls="--", lw=1.5, label="Target 34.2%")
ax_pm.set_title("Profit Margin %  —  Quarterly",
                fontsize=10, fontweight="bold", color=NAVY, pad=6)
ax_pm.set_xticks(range(12)); ax_pm.set_xticklabels(qtrs, fontsize=7, rotation=30)
ax_pm.set_ylim(28,38); ax_pm.yaxis.grid(True, alpha=0.25)
ax_pm.legend(fontsize=8)

# ── Pipeline status panel ─────────────────────────────────────────────────────
ax_ps = fig.add_axes([0.73, 0.060, 0.26, 0.310])
ax_ps.set_facecolor(WHITE); ax_ps.axis("off")
ax_ps.text(0.5, 0.97, "Pipeline Status — Today",
           ha="center", fontsize=10, fontweight="bold", color=NAVY)
stages_s  = ["Extraction","Transformation","Anomaly Detection","Report Generation"]
times_s   = ["06:00 · 1.2s","06:30 · 2.5s","06:31 · 0.9s","06:32 · 3.1s"]
for j,(st,tm) in enumerate(zip(stages_s, times_s)):
    y = 0.79 - j*0.185
    ax_ps.add_patch(FancyBboxPatch((0.03,y-0.06),0.94,0.14,
                    boxstyle="round,pad=0.01",facecolor=LGREEN,edgecolor=GREEN,lw=1.2))
    ax_ps.text(0.10, y+0.01, st,  fontsize=8.5, color=DARK, fontweight="bold")
    ax_ps.text(0.10, y-0.03, tm,  fontsize=7.5, color=GREY)
    ax_ps.text(0.92, y-0.01, "✔ Completed", fontsize=8, color=GREEN,
               fontweight="bold", ha="right")

plt.savefig("outputs/executive_overview_dashboard.png",
            dpi=150, bbox_inches="tight", facecolor=LGREY)
plt.close()
print("✅  executive_overview_dashboard.png")


# =============================================================================
# DASHBOARD 2 — Pipeline Run Log and Monitoring Panel
# Matches: "daily run timestamps, records processed per run,
#  processing duration, and status (Completed / Failed)"
# =============================================================================

fig = plt.figure(figsize=(22,14), facecolor=LGREY)

hdr = fig.add_axes([0,0.935,1,0.065])
hdr.set_facecolor(NAVY); hdr.axis("off")
hdr.add_patch(plt.Rectangle((0,0),0.006,1,color=GOLD))
hdr.text(0.012,0.65,"Automated Business Intelligence Reporting System",
         color=WHITE,fontsize=15,fontweight="bold",va="center")
hdr.text(0.012,0.25,"Pipeline Run Log & Monitoring Panel  ·  Power BI  ·  Last 30 Days",
         color=LBLUE,fontsize=9,va="center")
hdr.text(0.99,0.70,"Monitoring Date:  2024-12-31  06:35",
         color=GOLD,fontsize=9,va="center",ha="right")
hdr.text(0.99,0.30,"Source:  dbo.pipeline_log  (MS SQL Server)",
         color=LGREEN,fontsize=9,va="center",ha="right")

# KPI cards
run_kpis = [
    ("Total Runs (30d)", "30",    "Last: 2024-12-31", True,  NAVY),
    ("Successful",       "28",    "✔ 93.3% success",  True,  GREEN),
    ("Failed",           "2",     "▼ 6.7% failure",   False, RED),
    ("Avg Duration",     "7.7s",  "Target < 10s",     True,  BLUE),
    ("Avg Records/Run",  "10,791","▲ Stable",         True,  TEAL),
]
for i,(lbl,val,dlt,good,acc) in enumerate(run_kpis):
    kpi_card(fig,[0.01+i*0.197,0.790,0.185,0.120],lbl,val,dlt,good,acc)

# Run log table
ax_log = fig.add_axes([0.01,0.395,0.64,0.360])
ax_log.set_facecolor(WHITE)
ax_log.set_title("Daily Pipeline Run Log  —  Last 12 Executions  (dbo.pipeline_log)",
                 fontsize=10,fontweight="bold",color=NAVY,pad=8)
log_cols = ["Run Date","Stage","Status","Records","Duration","Start Time","End Time"]
log_rows = [
    ["2024-12-31","Full Pipeline","✔ Completed","10,800","7.7s","06:00:00","06:00:08"],
    ["2024-12-30","Full Pipeline","✔ Completed","10,785","7.5s","06:00:00","06:00:08"],
    ["2024-12-29","Full Pipeline","✔ Completed","10,791","7.6s","06:00:00","06:00:08"],
    ["2024-12-28","Full Pipeline","✔ Completed","10,802","7.8s","06:00:00","06:00:08"],
    ["2024-12-27","Full Pipeline","✔ Completed","10,788","7.4s","06:00:00","06:00:08"],
    ["2024-12-26","Full Pipeline","✔ Completed","10,775","7.3s","06:00:00","06:00:08"],
    ["2024-12-25","Full Pipeline","✔ Completed","10,800","7.9s","06:00:00","06:00:08"],
    ["2024-12-24","Full Pipeline","✔ Completed","10,812","7.6s","06:00:00","06:00:08"],
    ["2024-12-23","Full Pipeline","✔ Completed","10,796","7.5s","06:00:00","06:00:08"],
    ["2024-12-22","Extraction",   "✘ Failed",   "0",    "1.2s","06:00:00","06:00:01"],
    ["2024-12-21","Full Pipeline","✔ Completed","10,799","7.7s","06:00:00","06:00:08"],
    ["2024-12-20","Full Pipeline","✔ Completed","10,788","7.4s","06:00:00","06:00:08"],
]
draw_table(ax_log,log_cols,log_rows,
           col_widths=[0.135,0.140,0.125,0.090,0.090,0.120,0.120],
           x0=0.005,y0=0.90,row_h=0.071,header_h=0.085,font=8)

# Duration bar chart
ax_dur = fig.add_axes([0.67,0.590,0.315,0.165])
ax_dur.set_facecolor(WHITE)
days   = [f"D{20+i}" for i in range(12)]
durs   = [7.4,7.7,7.5,7.6,7.8,7.4,7.3,7.9,7.6,1.2,7.7,7.7]
bcols  = [RED if d<2 else BLUE for d in durs]
ax_dur.bar(range(12), durs, color=bcols, width=0.62, edgecolor=WHITE)
ax_dur.axhline(7.7, color=ORANGE, ls="--", lw=1.5, label="Avg 7.7s")
ax_dur.set_title("Run Duration (seconds)  —  Last 12 Days",
                 fontsize=10, fontweight="bold", color=NAVY, pad=6)
ax_dur.set_xticks(range(12)); ax_dur.set_xticklabels(days, fontsize=8)
ax_dur.set_ylim(0,11); ax_dur.yaxis.grid(True, alpha=0.25)
ax_dur.legend(fontsize=8)
ax_dur.annotate("Connection\nTimeout", xy=(9,1.2), xytext=(9,4.5),
                arrowprops=dict(arrowstyle="->",color=RED),
                fontsize=7.5, color=RED, ha="center")

# Stage breakdown
ax_stg = fig.add_axes([0.67,0.395,0.315,0.160])
ax_stg.set_facecolor(WHITE)
st_names = ["Extraction","Transformation","Anomaly Det.","Report Gen."]
st_times = [1.2, 2.5, 0.9, 3.1]
st_cols  = [TEAL, BLUE, ORANGE, NAVY]
brs = ax_stg.barh(st_names, st_times, color=st_cols, height=0.5, edgecolor=WHITE)
for b,v in zip(brs, st_times):
    ax_stg.text(v+0.05, b.get_y()+b.get_height()/2,
                f"{v}s", va="center", fontsize=9, color=DARK, fontweight="bold")
ax_stg.set_title("Average Stage Duration (seconds)",
                 fontsize=10, fontweight="bold", color=NAVY, pad=6)
ax_stg.set_xlim(0,5.5); ax_stg.xaxis.grid(True, alpha=0.25)

# Records processed trend
ax_rec = fig.add_axes([0.01,0.060,0.50,0.300])
ax_rec.set_facecolor(WHITE)
days30 = list(range(1,31))
recs   = [10790+np.random.randint(-30,30) for _ in range(29)] + [10800]
recs[9] = 0  # failed run
ax_rec.fill_between(days30, recs, alpha=0.12, color=BLUE)
ax_rec.plot(days30, recs, color=BLUE, lw=2, marker="o", ms=4)
ax_rec.scatter([10], [0], color=RED, zorder=5, s=80, label="Failed run")
ax_rec.axhline(10800, color=GREEN, ls="--", lw=1.5, label="Expected 10,800")
ax_rec.set_title("Records Processed per Run  —  Last 30 Days",
                 fontsize=10, fontweight="bold", color=NAVY, pad=6)
ax_rec.set_xlabel("Day of Month", fontsize=9)
ax_rec.set_ylabel("Records Processed", fontsize=9)
ax_rec.set_ylim(-500, 12000); ax_rec.yaxis.grid(True, alpha=0.25)
ax_rec.legend(fontsize=8)

# Success rate donut
ax_dr = fig.add_axes([0.54,0.060,0.18,0.300])
ax_dr.set_facecolor(WHITE)
wedges,_ = ax_dr.pie([28,2], colors=[GREEN, RED], startangle=90,
                      wedgeprops=dict(width=0.48, edgecolor=WHITE, lw=2))
ax_dr.text(0, 0, "93.3%\nSuccess", ha="center", va="center",
           fontsize=11, fontweight="bold", color=NAVY)
ax_dr.set_title("30-Day\nSuccess Rate",
                fontsize=10, fontweight="bold", color=NAVY, pad=6)
leg_patches = [mpatches.Patch(color=GREEN, label="Completed (28)"),
               mpatches.Patch(color=RED,   label="Failed (2)")]
ax_dr.legend(handles=leg_patches, fontsize=8, loc="lower center",
             bbox_to_anchor=(0.5,-0.08))

# Stage success table
ax_ss = fig.add_axes([0.73,0.060,0.26,0.300])
ax_ss.set_facecolor(WHITE)
ax_ss.set_title("Stage-Level Success Summary",
                fontsize=10, fontweight="bold", color=NAVY, pad=6)
ss_cols = ["Stage","Runs","Success","Failed"]
ss_rows = [
    ["Extraction",      "30","28","2"],
    ["Transformation",  "28","28","0"],
    ["Anomaly Det.",    "28","28","0"],
    ["Report Gen.",     "28","28","0"],
]
draw_table(ax_ss, ss_cols, ss_rows,
           col_widths=[0.37,0.21,0.21,0.21],
           x0=0.01, y0=0.86, row_h=0.145, header_h=0.12, font=8.5)

plt.savefig("outputs/pipeline_run_log_monitoring.png",
            dpi=150, bbox_inches="tight", facecolor=LGREY)
plt.close()
print("✅  pipeline_run_log_monitoring.png")


# =============================================================================
# DASHBOARD 3 — Automated Report Output: Sample Management Report
# Matches: "formatted KPI tables, trend charts, and summary outputs
#  produced without manual intervention"
# =============================================================================

fig = plt.figure(figsize=(22,15), facecolor=WHITE)

# Page header — looks like a real generated report
hdr = fig.add_axes([0,0.945,1,0.055])
hdr.set_facecolor(NAVY); hdr.axis("off")
hdr.add_patch(plt.Rectangle((0,0),0.006,1,color=GOLD))
hdr.text(0.012,0.72,"AUTOMATED BUSINESS INTELLIGENCE REPORTING SYSTEM",
         color=WHITE,fontsize=14,fontweight="bold",va="center")
hdr.text(0.012,0.25,"Management Report  ·  Reporting Period: January – December 2024",
         color=GOLD,fontsize=9,va="center")
hdr.text(0.988,0.72,"Generated: 2024-12-31  06:32  (Automated — No Manual Intervention)",
         color=WHITE,fontsize=8.5,va="center",ha="right")
hdr.text(0.988,0.25,"Pipeline Run #96  ·  Source: dbo.rpt_*  ·  Power BI Service",
         color=LBLUE,fontsize=8,va="center",ha="right")

# Section 1 band
s1 = fig.add_axes([0,0.895,1,0.040])
s1.set_facecolor(LBLUE); s1.axis("off")
s1.text(0.01,0.5,"SECTION 1  —  EXECUTIVE KEY PERFORMANCE INDICATORS",
        color=NAVY,fontsize=11,fontweight="bold",va="center")

# KPI summary table
ax_kt = fig.add_axes([0.01,0.670,0.56,0.205])
ax_kt.set_facecolor(WHITE)
ax_kt.set_title("Annual KPI Summary  —  Fiscal Year 2022 | 2023 | 2024",
                fontsize=10,fontweight="bold",color=NAVY,pad=6,loc="left")
kt_cols = ["KPI Metric","2022","2023","2024","YoY Change","Status"]
kt_rows = [
    ["Total Revenue ($M)",       "$24.1","$25.9","$28.4","+$2.5M  +9.7%",  "✔ Achieved"],
    ["Gross Profit ($M)",        "$7.6", "$8.0", "$9.7", "+$1.7M  +21.4%", "✔ Achieved"],
    ["Profit Margin (%)",        "31.5%","30.9%","34.2%","+3.3pp",          "✔ Achieved"],
    ["Total Transactions",       "10,800","10,800","10,800","Stable",        "✔ On Target"],
    ["Revenue / Transaction",    "$2,231","$2,398","$2,630","+$232  +9.7%", "✔ Achieved"],
    ["Enterprise Revenue Share", "40.2%","41.8%","43.0%","+1.2pp",          "✔ Achieved"],
    ["Avg Discount %",           "10.4%","10.1%","9.8%", "-0.3pp",          "✔ Achieved"],
    ["Anomalies Detected",       "218",  "204",  "216",  "+12  +5.9%",      "⚠ Monitor"],
]
draw_table(ax_kt, kt_cols, kt_rows,
           col_widths=[0.26,0.09,0.09,0.09,0.19,0.15],
           x0=0.005, y0=0.93, row_h=0.098, header_h=0.095, font=8)

# Quarterly revenue grouped bar
ax_qr = fig.add_axes([0.59,0.670,0.40,0.205])
ax_qr.set_facecolor(WHITE)
qs  = ["Q1","Q2","Q3","Q4"]
r22q = [5.65,6.55,5.85,6.05]
r23q = [6.22,6.02,6.73,6.93]
r24q = [6.65,7.05,7.05,7.65]
x = np.arange(4); w = 0.26
ax_qr.bar(x-w, r22q, w, label="2022", color=LBLUE, edgecolor=WHITE)
ax_qr.bar(x,   r23q, w, label="2023", color=BLUE,  edgecolor=WHITE)
ax_qr.bar(x+w, r24q, w, label="2024", color=NAVY,  edgecolor=WHITE)
ax_qr.set_xticks(x); ax_qr.set_xticklabels(qs, fontsize=10)
ax_qr.set_title("Quarterly Revenue Comparison ($M)",
                fontsize=10,fontweight="bold",color=NAVY,pad=6)
ax_qr.legend(fontsize=9); ax_qr.yaxis.grid(True,alpha=0.25)
ax_qr.set_ylabel("Revenue ($M)",fontsize=9)
for bar in ax_qr.patches:
    if bar.get_height()>0:
        ax_qr.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.05,
                   f"${bar.get_height():.1f}",ha="center",fontsize=6.5,color=DARK)

# Section 2 band
s2 = fig.add_axes([0,0.630,1,0.033])
s2.set_facecolor(LBLUE); s2.axis("off")
s2.text(0.01,0.5,"SECTION 2  —  REGIONAL PERFORMANCE SUMMARY",
        color=NAVY,fontsize=11,fontweight="bold",va="center")

# Regional table
ax_rgt = fig.add_axes([0.01,0.415,0.98,0.198])
ax_rgt.set_facecolor(WHITE)
ax_rgt.set_title("Regional Performance  —  Fiscal Year 2024",
                 fontsize=10,fontweight="bold",color=NAVY,pad=6,loc="left")
rg_cols = ["Region","Revenue 2024","Revenue 2023","YoY Growth",
           "Profit Margin","Transactions","Rev Share","Top Sales Rep"]
rg_rows = [
    ["East",         "$7.20M","$6.10M","▲ +18.0%","35.4%","2,476","25.4%","Maria Garcia"],
    ["North",        "$6.40M","$5.60M","▲ +14.3%","33.8%","2,171","22.5%","Alex Johnson"],
    ["West",         "$5.90M","$5.10M","▲ +15.7%","34.1%","2,050","20.8%","David Chen"],
    ["South",        "$5.30M","$4.60M","▲ +15.2%","32.9%","1,987","18.7%","Sarah Williams"],
    ["International","$3.60M","$2.90M","▲ +24.1%","36.8%","2,116","12.7%","James Martinez"],
    ["TOTAL",        "$28.40M","$24.30M","▲ +16.9%","34.2%","10,800","100%",""],
]
draw_table(ax_rgt, rg_cols, rg_rows,
           col_widths=[0.10,0.11,0.11,0.11,0.11,0.11,0.10,0.18],
           x0=0.005, y0=0.93, row_h=0.126, header_h=0.110, font=8.5)

# Section 3 band
s3 = fig.add_axes([0,0.378,1,0.030])
s3.set_facecolor(LBLUE); s3.axis("off")
s3.text(0.01,0.5,"SECTION 3  —  PRODUCT & CHANNEL ANALYSIS",
        color=NAVY,fontsize=11,fontweight="bold",va="center")

# Product revenue bar
ax_pr = fig.add_axes([0.01,0.075,0.27,0.278])
ax_pr.set_facecolor(WHITE)
cats  = ["Software","Services","Electronics","Hardware"]
catv  = [10.2, 8.8, 5.3, 4.1]
brs = ax_pr.barh(cats, catv, color=[NAVY,BLUE,TEAL,ORANGE], height=0.52, edgecolor=WHITE)
for b,v in zip(brs,catv):
    ax_pr.text(v+0.15, b.get_y()+b.get_height()/2,
               f"${v}M", va="center", fontsize=9, fontweight="bold", color=DARK)
ax_pr.set_title("Revenue by Product Category ($M)",
                fontsize=10,fontweight="bold",color=NAVY,pad=6)
ax_pr.set_xlim(0,14); ax_pr.xaxis.grid(True,alpha=0.25)

# Channel margin
ax_cm = fig.add_axes([0.30,0.075,0.22,0.278])
ax_cm.set_facecolor(WHITE)
chn  = ["Direct","Partner","Online"]
chmg = [33.5, 35.1, 34.8]
brs2 = ax_cm.bar(chn, chmg, color=[NAVY,BLUE,TEAL], width=0.5, edgecolor=WHITE)
for b,v in zip(brs2,chmg):
    ax_cm.text(b.get_x()+b.get_width()/2, v+0.15,
               f"{v}%", ha="center", fontsize=9, fontweight="bold", color=DARK)
ax_cm.set_title("Profit Margin % by Channel",
                fontsize=10,fontweight="bold",color=NAVY,pad=6)
ax_cm.set_ylim(30,38); ax_cm.yaxis.grid(True,alpha=0.25)

# Product top 5 table
ax_tp = fig.add_axes([0.54,0.075,0.45,0.278])
ax_tp.set_facecolor(WHITE)
ax_tp.set_title("Top 5 Products by Revenue  —  2024",
                fontsize=10,fontweight="bold",color=NAVY,pad=6,loc="left")
tp_cols = ["Product","Category","Revenue","Gross Profit","Margin %","Units"]
tp_rows = [
    ["ERP Module",          "Software",    "$4.82M","$3.86M","80.1%","803"],
    ["Implementation",      "Services",    "$4.10M","$2.46M","60.0%","513"],
    ["Analytics Platform",  "Software",    "$3.60M","$2.95M","82.0%","1,000"],
    ["Server Rack Unit",    "Hardware",    "$3.21M","$0.67M","20.9%","583"],
    ["CRM Suite – Annual",  "Software",    "$2.88M","$2.52M","87.5%","1,200"],
]
draw_table(ax_tp, tp_cols, tp_rows,
           col_widths=[0.25,0.17,0.13,0.13,0.12,0.10],
           x0=0.005, y0=0.90, row_h=0.152, header_h=0.130, font=8.5)

# Footer
ftr = fig.add_axes([0,0,1,0.032])
ftr.set_facecolor(NAVY); ftr.axis("off")
ftr.text(0.5,0.5,
         "This report was generated automatically by the BI Pipeline on 2024-12-31 at 06:32  "
         "·  Source: dbo.rpt_monthly_revenue · dbo.rpt_regional_summary · dbo.rpt_product_summary  "
         "·  No manual intervention required",
         ha="center",va="center",fontsize=7.5,color=GOLD)

plt.savefig("outputs/automated_management_report.png",
            dpi=150, bbox_inches="tight", facecolor=WHITE)
plt.close()
print("✅  automated_management_report.png")


# =============================================================================
# DASHBOARD 4 — Data Quality Validation Report
# Matches: "null value counts, validation pass/fail status,
#  and data completeness metrics per pipeline run"
# =============================================================================

fig = plt.figure(figsize=(22,14), facecolor=LGREY)

hdr = fig.add_axes([0,0.935,1,0.065])
hdr.set_facecolor(NAVY); hdr.axis("off")
hdr.add_patch(plt.Rectangle((0,0),0.006,1,color=GOLD))
hdr.text(0.012,0.65,"Automated Business Intelligence Reporting System",
         color=WHITE,fontsize=15,fontweight="bold",va="center")
hdr.text(0.012,0.25,"Data Quality Validation Report  ·  Power BI  ·  Staging Layer Monitoring",
         color=LBLUE,fontsize=9,va="center")
hdr.text(0.99,0.70,"Run Date:  2024-12-31  |  Stage:  Staging Validation",
         color=GOLD,fontsize=9,va="center",ha="right")
hdr.text(0.99,0.30,"Source:  dbo.pipeline_log  (MS SQL Server)",
         color=LGREEN,fontsize=9,va="center",ha="right")

# Quality KPI cards
q_kpis = [
    ("Overall Quality",    "98.2%",  "▲ Above 98% threshold", True,  GREEN),
    ("Rows Loaded",        "10,800", "✔ Full load",           True,  NAVY),
    ("Rows Rejected",      "193",    "▼ 1.8% rejection rate", False, ORANGE),
    ("Issues Auto-Fixed",  "47",     "✔ Corrected in-pipeline",True, BLUE),
    ("Duplicates Removed", "0",      "✔ Clean source",        True,  TEAL),
]
for i,(lbl,val,dlt,good,acc) in enumerate(q_kpis):
    kpi_card(fig,[0.01+i*0.197,0.795,0.185,0.115],lbl,val,dlt,good,acc)

# Validation checks table
ax_vt = fig.add_axes([0.01,0.435,0.58,0.330])
ax_vt.set_facecolor(WHITE)
ax_vt.set_title("Automated Validation Check Results  —  2024-12-31",
                fontsize=10,fontweight="bold",color=NAVY,pad=8)
vt_cols = ["Validation Check","Rows Affected","Action Taken","Result"]
vt_rows = [
    ["NULL transaction_id",     "0",   "Reject row",           "✔ PASS"],
    ["NULL record_date",        "0",   "Reject row",           "✔ PASS"],
    ["NULL revenue",            "0",   "Reject row",           "✔ PASS"],
    ["Negative revenue",        "193", "Reject row",           "⚠ WARN"],
    ["Negative cost",           "0",   "Reject row",           "✔ PASS"],
    ["Discount > 100%",         "12",  "Clamped to 100%",      "✔ FIXED"],
    ["Discount < 0%",           "8",   "Clamped to 0%",        "✔ FIXED"],
    ["Units sold ≤ 0",          "27",  "Defaulted to 1",       "✔ FIXED"],
    ["NULL cost",               "0",   "Defaulted to 0",       "✔ PASS"],
    ["Duplicate transaction_id","0",   "Deduplicated",         "✔ PASS"],
    ["Invalid region value",    "0",   "Standardised case",    "✔ PASS"],
    ["Invalid channel value",   "0",   "Standardised case",    "✔ PASS"],
]
draw_table(ax_vt, vt_cols, vt_rows,
           col_widths=[0.36,0.17,0.27,0.16],
           x0=0.01, y0=0.93, row_h=0.072, header_h=0.080, font=8)

# Null count bar chart
ax_nc = fig.add_axes([0.62,0.590,0.365,0.175])
ax_nc.set_facecolor(WHITE)
nc_cols = ["revenue","cost","units_sold","discount_pct","product_name","region","channel"]
nc_vals = [0, 0, 27, 20, 0, 0, 0]
nc_c    = [GREEN if v==0 else ORANGE for v in nc_vals]
ax_nc.bar(nc_cols, nc_vals, color=nc_c, width=0.55, edgecolor=WHITE)
for i,v in enumerate(nc_vals):
    ax_nc.text(i, v+0.4, str(v), ha="center", fontsize=9, fontweight="bold", color=DARK)
ax_nc.set_title("Null Value Count by Column",
                fontsize=10,fontweight="bold",color=NAVY,pad=6)
ax_nc.tick_params(axis='x', labelrotation=20, labelsize=8)
ax_nc.set_ylim(0,38); ax_nc.yaxis.grid(True,alpha=0.25)
green_p = mpatches.Patch(color=GREEN,  label="0 nulls (clean)")
org_p   = mpatches.Patch(color=ORANGE, label="Nulls detected")
ax_nc.legend(handles=[green_p,org_p], fontsize=8)

# Data completeness horizontal bars
ax_cmp = fig.add_axes([0.62,0.435,0.365,0.120])
ax_cmp.set_facecolor(WHITE)
cmp_c = ["transaction_id","record_date","revenue","cost","region","channel","sales_rep"]
cmp_v = [100,100,98.2,100,100,100,100]
cmp_cols = [GREEN if v==100 else ORANGE for v in cmp_v]
ax_cmp.barh(cmp_c, cmp_v, color=cmp_cols, height=0.52, edgecolor=WHITE)
ax_cmp.axvline(100,color=GREY,ls="--",lw=1)
for i,v in enumerate(cmp_v):
    ax_cmp.text(v+0.1, i, f"{v}%", va="center", fontsize=8.5, fontweight="bold", color=DARK)
ax_cmp.set_title("Data Completeness % by Column",
                 fontsize=10,fontweight="bold",color=NAVY,pad=6)
ax_cmp.set_xlim(90,104)
ax_cmp.tick_params(axis='y', labelsize=8)
ax_cmp.xaxis.grid(True,alpha=0.25)

# Quality score trend 30 days
ax_qt = fig.add_axes([0.01,0.075,0.53,0.325])
ax_qt.set_facecolor(WHITE)
np.random.seed(42)
d30  = list(range(1,31))
qs30 = [round(98.5+np.random.uniform(-0.6,0.8),1) for _ in range(29)] + [98.2]
qs30[9] = 0.0   # failed run — no quality score
valid_d  = [d for d,q in zip(d30,qs30) if q>0]
valid_q  = [q for q in qs30 if q>0]
ax_qt.fill_between(valid_d, valid_q, 97.5, alpha=0.12, color=GREEN)
ax_qt.plot(valid_d, valid_q, color=GREEN, lw=2.5, marker="o", ms=4, label="Quality Score %")
ax_qt.scatter([10],[0], color=RED, zorder=5, s=90, label="Pipeline failure (no score)")
ax_qt.axhline(98.0, color=ORANGE, ls="--", lw=1.5, label="Min Threshold  98.0%")
ax_qt.set_title("Data Quality Score  —  Last 30 Days",
                fontsize=10,fontweight="bold",color=NAVY,pad=6)
ax_qt.set_xlabel("Day of Month",fontsize=9)
ax_qt.set_ylabel("Quality Score (%)",fontsize=9)
ax_qt.set_ylim(95,101); ax_qt.yaxis.grid(True,alpha=0.25)
ax_qt.legend(fontsize=8)

# Pipeline log quality extract
ax_ql = fig.add_axes([0.56,0.075,0.43,0.325])
ax_ql.set_facecolor(WHITE)
ax_ql.set_title("dbo.pipeline_log  —  Staging Stage Extract",
                fontsize=10,fontweight="bold",color=NAVY,pad=6)
ql_cols = ["run_date","status","records","message"]
ql_rows = [
    ["2024-12-31","✔ Completed","10,800","Staging clean — 10,800 rows validated"],
    ["2024-12-30","✔ Completed","10,785","Staging clean — 10,785 rows validated"],
    ["2024-12-29","✔ Completed","10,791","Staging clean — 10,791 rows validated"],
    ["2024-12-28","✔ Completed","10,802","Staging clean — 10,802 rows validated"],
    ["2024-12-27","✔ Completed","10,788","Staging clean — 10,788 rows validated"],
    ["2024-12-22","✘ Failed",   "0",     "Connection timeout: Server unavailable"],
    ["2024-12-21","✔ Completed","10,799","Staging clean — 10,799 rows validated"],
]
draw_table(ax_ql, ql_cols, ql_rows,
           col_widths=[0.18,0.18,0.14,0.48],
           x0=0.01, y0=0.90, row_h=0.112, header_h=0.095, font=8)

# Footer
ftr = fig.add_axes([0,0,1,0.032])
ftr.set_facecolor(NAVY); ftr.axis("off")
ftr.text(0.5,0.5,
         "Data quality validation executed automatically at pipeline stage 'Staging'  "
         "·  Results written to dbo.pipeline_log  ·  Quality threshold: ≥ 98.0%  "
         "·  Auto-fix applied to non-critical issues",
         ha="center",va="center",fontsize=7.5,color=GOLD)

plt.savefig("outputs/data_quality_validation_report.png",
            dpi=150, bbox_inches="tight", facecolor=LGREY)
plt.close()
print("✅  data_quality_validation_report.png")

print("\n✅  All 4 dashboard PNGs generated in outputs/")
