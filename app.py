import streamlit as st
import asyncio
import pandas as pd
import time
from pipeline_controller import RefineryPipeline
from workflow_db import WorkflowDB

st.set_page_config(page_title="Autonomous Data Refinery", layout="wide", page_icon="⚙️")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: #c9d1d9; }
    
    /* Terminal Box */
    .terminal-box {
        font-family: 'Courier New', monospace;
        background-color: #0d1117;
        color: #3fb950;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #30363d;
        height: 400px;
        overflow-y: auto;
        font-size: 0.85em;
        white-space: pre-wrap;
        display: flex;
        flex-direction: column-reverse; /* Keeps new logs at bottom */
    }
    
    /* Progress Bar Steps */
    .step-container {
        display: flex;
        justify-content: space-between;
        margin-bottom: 20px;
        background: #161b22;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #30363d;
    }
    .step-item {
        text-align: center;
        color: #484f58; /* Dimmed Gray */
        font-weight: bold;
        flex: 1;
        transition: all 0.3s;
    }
    .step-active { 
        color: #58a6ff; /* Blue Glow */
        text-shadow: 0 0 10px rgba(88, 166, 255, 0.5);
        border-bottom: 2px solid #58a6ff;
    }
    .step-done {
        color: #2ea043; /* Green Check */
    }
</style>
""", unsafe_allow_html=True)

# --- INITIALIZATION ---
if 'pipeline' not in st.session_state: st.session_state.pipeline = RefineryPipeline()
if 'logs' not in st.session_state: st.session_state.logs = []
if 'running' not in st.session_state: st.session_state.running = False
if 'current_phase' not in st.session_state: st.session_state.current_phase = 0

db = WorkflowDB()

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configuration")
    uploaded_file = st.file_uploader("Upload doctor_names.txt", type=['txt'])
    
    st.divider()
    c1, c2 = st.columns(2)
    
    if c1.button("🗑️ Wipe DB"):
        db.clear_database()
        st.session_state.logs = []
        st.session_state.current_phase = 0
        st.success("Database Wiped!")
        time.sleep(0.5)
        st.rerun()
        
    if c2.button("🧹 Clear Logs"):
        st.session_state.logs = []
        st.rerun()

# --- MAIN DASHBOARD ---
st.title("🏭 Autonomous Data Refinery")

# Metrics
df = db.get_dataframe()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Records", len(df) if not df.empty else 0)
c2.metric("Verified", len(df[df['status']=='Verified']) if not df.empty else 0)
c3.metric("Enriched", len(df[df['status']=='Enriched']) if not df.empty else 0)
c4.metric("Manual Review", len(df[df['status']=='Manual_Review']) if not df.empty else 0)

st.divider()

# --- DYNAMIC PROGRESS BAR ---
def render_progress_bar(phase):
    # 1=Scraping, 2=Scoring, 3=Enrichment, 4=Verification
    steps = ["Scraping", "Scoring", "Enrichment", "Verification"]
    html = '<div class="step-container">'
    
    for i, step in enumerate(steps, 1):
        c_class = "step-item"
        if i < phase: c_class += " step-done"   # Past steps are Green
        elif i == phase: c_class += " step-active" # Current step is Blue
        
        html += f'<div class="{c_class}">{i}. {step}</div>'
    html += '</div>'
    return html

# Placeholder for the bar so we can update it
prog_placeholder = st.empty()
prog_placeholder.markdown(render_progress_bar(st.session_state.current_phase), unsafe_allow_html=True)

# --- CONTROLS & TERMINAL ---
col_ctrl, col_term = st.columns([1, 1])

with col_ctrl:
    st.subheader("🕹️ Mission Control")
    if st.button("▶ START AUTO MODE", disabled=st.session_state.running, type="primary"):
        if uploaded_file:
            st.session_state.running = True
            st.rerun()
        else:
            st.error("⚠️ Upload a file first!")

    if st.button("⏹ EMERGENCY STOP"):
        st.session_state.pipeline.stop()
        st.session_state.running = False
        st.error("Stopping...")

with col_term:
    st.subheader("💻 Live Terminal")
    term_placeholder = st.empty()
    
    def update_terminal():
        # Show last 15 lines
        content = "\n".join(st.session_state.logs[-20:])
        term_placeholder.markdown(f'<div class="terminal-box">{content}</div>', unsafe_allow_html=True)
    
    update_terminal()

# --- PIPELINE EXECUTION ---
if st.session_state.running and uploaded_file:
    doctors = uploaded_file.getvalue().decode("utf-8").splitlines()
    doctors = [d.strip() for d in doctors if d.strip()]
    
    async def run_loop():
        # Listen to the generator
        async for msg in st.session_state.pipeline.run(doctors):
            
            # 1. Update Progress Bar if it's a PHASE signal
            if msg.startswith("PHASE:"):
                try:
                    p_num = int(msg.split(":")[1])
                    st.session_state.current_phase = p_num
                    prog_placeholder.markdown(render_progress_bar(p_num), unsafe_allow_html=True)
                except: pass
            
            # 2. Update Terminal for normal messages
            else:
                timestamp = time.strftime("%H:%M:%S")
                log_entry = f"[{timestamp}] {msg}"
                st.session_state.logs.append(log_entry)
                update_terminal()
            
    asyncio.run(run_loop())
    
    st.session_state.running = False
    st.success("✅ Workflow Complete!")
    time.sleep(1)
    st.rerun()

# --- DATABASE TABLE ---
st.divider()
with st.expander("🗄️ View Complete Database", expanded=True):
    if not df.empty:
        st.dataframe(
            df, 
            use_container_width=True,
            column_config={
                "initial_score": st.column_config.ProgressColumn("Init Score", format="%.2f", min_value=0, max_value=1),
                "final_score": st.column_config.ProgressColumn("Final Score", format="%.2f", min_value=0, max_value=1),
            }
        )
    else:
        st.info("Database is empty. Upload a file and start the pipeline.")
