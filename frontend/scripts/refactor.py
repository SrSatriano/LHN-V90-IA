import re

file_path = "D:\\Desenvolvimento\\IA de Finanças\\LHN_Sovereign_Collection-Git\\LHN_Sovereign_V90 - Backup\\frontend\\components\\CryptoWorkspace.tsx"

with open(file_path, "r", encoding="utf-8") as f:
    text = f.read()

print(f"File loaded, length: {len(text)}")

# 1. Remove Modal
modal_start_rx = re.compile(
    r"\{\/\* --- Settings Modal V87 Style \(3 abas\) ---\s*\*\/.*?\{\s*isSettingsModalOpen && \(\s*<div[^>]*>.*?<div className=\"bg-\[\#131722\] border border-\[\#2A2E39\] rounded-2xl shadow-2xl w-full max-w-2xl flex flex-col\" style=\{\{maxHeight:'92vh'\}\}>",
    re.DOTALL,
)

modal_match = modal_start_rx.search(text)
if modal_match:
    settings_inner_regex = re.compile(
        r"(<div className=\"flex items-center border-b border-\[\#2A2E39\] shrink-0\">.*?)</div>\s*</div>\s*\)\}",
        re.DOTALL,
    )
    m_settings = settings_inner_regex.search(text[modal_match.end() :])
    if m_settings:
        settings_content = m_settings.group(1)
        text = (
            text[: modal_match.start()] + text[modal_match.end() + m_settings.end() :]
        )
        print("Extracted settings modal")
    else:
        print("Failed to match inner settings")
else:
    print("Modal start not found!")

# 2. Extract Bottom Tab Terminal and IA
terminal_ia_regex = re.compile(
    r"(\) : activeTab === \"terminal\" \? \(\s*<div className=\"flex-1 overflow-y-auto p-4 font-mono text-xs text-gray-400 custom-scrollbar whitespace-pre-wrap h-full\".*?)(\) : activeTab === \"history\" \? \()",
    re.DOTALL,
)

m_tabs = terminal_ia_regex.search(text)
if m_tabs:
    terminal_ia_content = m_tabs.group(1)

    term_regex = re.compile(
        r"\) : activeTab === \"terminal\" \? \(\s*(<div className=\"flex-1 overflow-y-auto p-4 font-mono text-xs text-gray-400 custom-scrollbar whitespace-pre-wrap h-full\".*?</div>)\s*\) : activeTab === \"ia\" \? \(",
        re.DOTALL,
    )
    ia_regex = re.compile(
        r"\) : activeTab === \"ia\" \? \(\s*(<div className=\"flex flex-col h-full bg-\[\#0b0e11\]\">.*?</div>)\s*$",
        re.DOTALL,
    )

    t_m = term_regex.search(terminal_ia_content)
    i_m = ia_regex.search(terminal_ia_content)

    term_inner = t_m.group(1) if t_m else "<div>Terminal Error</div>"
    ia_inner = i_m.group(1) if i_m else "<div>IA Error</div>"

    text = text[: m_tabs.start()] + m_tabs.group(2) + text[m_tabs.end() :]
    print("Extracted terminal and ia bottom tabs")
else:
    print("Terminal / IA tabs not found in bottom panel!")
    term_inner = "<div>Terminal Error</div>"
    ia_inner = "<div>IA Error</div>"

# 3. Inject them into the Main Top Level
horizonte_end_regex = re.compile(
    r"(\s*</div>\s*\) : \(\s*)(<div className=\"flex-1 flex overflow-x-auto overflow-y-hidden custom-scrollbar\">)"
)
h_match = horizonte_end_regex.search(text)

if h_match:
    # Need to be careful with escaping regex backreferences in replacement strings when replacement strings have lots of backslashes.
    # We will build the string directly.
    part1_idx = h_match.start(2)

    insertion = f""") : activeTab === "settings" ? (
         <div className="flex-1 overflow-y-auto p-4 md:p-8 custom-scrollbar space-y-6 animate-in fade-in duration-300 bg-[#0b0e11] w-full h-full relative">
            <div className="max-w-4xl mx-auto bg-[#131722] border border-[#2A2E39] rounded-2xl flex flex-col min-h-[85vh]">
               {settings_content}
            </div>
         </div>
      ) : activeTab === "terminal" ? (
         <div className="flex-1 overflow-y-auto p-4 md:p-8 custom-scrollbar animate-in fade-in duration-300 bg-[#0b0e11] w-full h-full relative">
            <div className="max-w-5xl mx-auto bg-[#131722] border border-[#2A2E39] rounded-2xl shadow-xl flex flex-col h-[85vh] overflow-hidden">
               <div className="px-6 py-4 border-b border-[#2A2E39] bg-[#1a1e28]">
                 <h2 className="text-xl font-bold flex items-center gap-2 text-white">Log de Informações</h2>
               </div>
               {term_inner}
            </div>
         </div>
      ) : activeTab === "ia" ? (
         <div className="flex-1 overflow-y-auto p-4 md:p-8 custom-scrollbar animate-in fade-in duration-300 bg-[#0b0e11] w-full h-full relative">
            <div className="max-w-5xl mx-auto bg-[#131722] border border-[#2A2E39] rounded-2xl shadow-xl flex flex-col h-[85vh] overflow-hidden">
               <div className="px-6 py-4 border-b border-[#2A2E39] bg-[#1a1e28]">
                 <h2 className="text-xl font-bold flex items-center gap-2 text-white">Central IA Nexus</h2>
               </div>
               {ia_inner}
            </div>
         </div>
      ) : (
      """

    text = text[: h_match.start()] + "\n      " + insertion + text[h_match.start(2) :]
    print("Injected into top level view!")
else:
    print("Horizonte end not found for injection!")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(text)

print(f"Refactor Complete. New length: {len(text)}")
