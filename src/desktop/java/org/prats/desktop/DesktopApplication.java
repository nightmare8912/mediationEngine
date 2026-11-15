package org.prats.desktop;

import com.fasterxml.jackson.databind.JsonNode;
import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Standalone Swing operator console; knows only the platform TCP endpoint. */
public final class DesktopApplication {
    private static final Color NAVY = new Color(19, 31, 49), BLUE = new Color(43, 91, 220), BG = new Color(242, 245, 250);
    private final PlatformClient client = new PlatformClient();
    private final JFrame frame = new JFrame("Mediation Engine • Operations");
    private final CardLayout pages = new CardLayout();
    private final JPanel root = new JPanel(pages);
    private final JLabel feedback = new JLabel("Connect to your platform to begin.");
    private final JLabel connectionLabel = new JLabel("DISCONNECTED");
    private final JLabel accountLabel = new JLabel("No active session");
    private final JTextField host = new JTextField("127.0.0.1", 15), port = new JTextField("15000", 6), username = new JTextField(22);
    private final JPasswordField password = new JPasswordField(22);
    private final JButton connect = new JButton("Connect"), login = new JButton("Sign in"), register = new JButton("Create account");
    private final DefaultTableModel monitoring = model("JVM", "Role", "Platform link", "Health", "PID", "Host CPU", "JVM CPU", "Host memory", "Heap used / max", "In flight");
    private final JTable jvms = table(monitoring);
    private final JTextArea console = area();
    private final JTextField command = new JTextField("mng help");
    private final JLabel overviewStatus = new JLabel("Waiting for first snapshot");
    private boolean refreshing;
    private boolean authenticated;
    private final javax.swing.Timer refreshTimer = new javax.swing.Timer(15000, event -> refresh());

    private static DefaultTableModel model(String... columns) {
        return new DefaultTableModel(columns, 0) { @Override public boolean isCellEditable(int row,int col) { return false; } };
    }
    private static JTable table(DefaultTableModel model) {
        JTable table = new JTable(model); table.setRowHeight(36); table.setAutoCreateRowSorter(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION); table.setShowGrid(false);
        table.setIntercellSpacing(new Dimension(0,0)); table.getTableHeader().setReorderingAllowed(false);
        table.getTableHeader().setFont(new Font(Font.SANS_SERIF, Font.BOLD, 12));
        return table;
    }
    private static JTextArea area() {
        JTextArea text = new JTextArea(); text.setEditable(false); text.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        text.setMargin(new Insets(18,18,18,18)); text.setTabSize(4); return text;
    }
    private static JPanel panel(LayoutManager layout) { JPanel p = new JPanel(layout); p.setBackground(BG); return p; }
    private static JLabel heading(String text,int size) { JLabel label = new JLabel(text); label.setFont(new Font(Font.SANS_SERIF, Font.BOLD,size)); return label; }
    private static void onUI(Runnable action) { if (SwingUtilities.isEventDispatchThread()) action.run(); else SwingUtilities.invokeLater(action); }
    private <T> void receive(CompletableFuture<T> future, Consumer<T> success) {
        future.whenComplete((value,error) -> onUI(() -> {
            if (error != null) showError(error); else success.accept(value);
        }));
    }
    private void showError(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause()!=null) error=error.getCause();
        String message = error.getMessage()==null ? error.getClass().getSimpleName() : error.getMessage();
        feedback.setText(message); feedback.setForeground(new Color(174,45,56));
        console.append("\n[error] " + message + "\n");
    }
    private void note(String message) { feedback.setForeground(new Color(63,80,103)); feedback.setText(message); }

    public DesktopApplication() {
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE); frame.setMinimumSize(new Dimension(1040,700));
        frame.setSize(1280,820); frame.setLocationRelativeTo(null);
        JPanel header = panel(new BorderLayout(20,0)); header.setBackground(NAVY); header.setBorder(new EmptyBorder(20,26,20,26));
        JLabel brand = heading("MEDIATION / ENGINE",20); brand.setForeground(Color.WHITE); header.add(brand,BorderLayout.WEST);
        connectionLabel.setForeground(new Color(166,186,211)); header.add(connectionLabel,BorderLayout.EAST);
        root.add(authPage(),"auth"); root.add(dashboard(),"dashboard");
        JPanel footer = panel(new BorderLayout()); footer.setBorder(new EmptyBorder(10,24,12,24)); footer.add(feedback);
        frame.add(header,BorderLayout.NORTH); frame.add(root); frame.add(footer,BorderLayout.SOUTH);
        client.onDisconnected(() -> onUI(() -> { authenticated=false; refreshTimer.stop(); pages.show(root,"auth");
            connectionLabel.setText("DISCONNECTED"); connect.setEnabled(true); login.setEnabled(true); register.setEnabled(true);
            note("Platform connection closed. Reconnect and sign in."); }));
        frame.addWindowListener(new WindowAdapter() { @Override public void windowClosed(WindowEvent event) { refreshTimer.stop(); client.close(); } });
        frame.setVisible(true);
    }
    private JPanel authPage() {
        JPanel page=panel(new GridBagLayout()); JPanel card=new JPanel(); card.setLayout(new BoxLayout(card,BoxLayout.Y_AXIS));
        card.setBackground(Color.WHITE); card.setBorder(new EmptyBorder(34,42,34,42));
        card.add(heading("Your platform. One connection.",27)); card.add(Box.createVerticalStrut(10));
        card.add(new JLabel("Monitor JVMs, inspect health, and manage execution containers.")); card.add(Box.createVerticalStrut(30));
        card.add(heading("PLATFORM ENDPOINT",12)); card.add(Box.createVerticalStrut(8));
        JPanel endpoint=new JPanel(new FlowLayout(FlowLayout.LEFT,6,0)); endpoint.setOpaque(false);
        endpoint.add(host); endpoint.add(new JLabel(":")); endpoint.add(port); endpoint.add(connect); card.add(endpoint);
        card.add(Box.createVerticalStrut(26)); card.add(heading("OPERATOR ACCOUNT",12)); card.add(Box.createVerticalStrut(10));
        card.add(new JLabel("Username")); username.setMaximumSize(new Dimension(500,35)); card.add(username);
        card.add(Box.createVerticalStrut(12)); card.add(new JLabel("Password")); password.setMaximumSize(new Dimension(500,35)); card.add(password);
        card.add(Box.createVerticalStrut(20)); JPanel actions=new JPanel(new FlowLayout(FlowLayout.LEFT,8,0)); actions.setOpaque(false);
        actions.add(login); actions.add(register); card.add(actions); card.add(Box.createVerticalStrut(16));
        card.add(new JLabel("New here? Create an account with a password of 10–128 characters."));
        page.add(card); connect.addActionListener(event -> connect()); login.addActionListener(event -> authenticate(false));
        register.addActionListener(event -> authenticate(true)); password.addActionListener(event -> authenticate(false)); return page;
    }
    private void connect() {
        int tcpPort;
        try { tcpPort=Integer.parseInt(port.getText().trim()); if(tcpPort<1||tcpPort>65535) throw new NumberFormatException(); }
        catch(NumberFormatException error) { note("Enter a TCP port between 1 and 65535."); return; }
        connect.setEnabled(false); note("Connecting to platform…");
        client.connect(host.getText().trim(),tcpPort).whenComplete((ignored,error)->onUI(()-> {
            connect.setEnabled(true); if(error!=null) { showError(error); return; }
            connectionLabel.setText("CONNECTED  •  "+host.getText().trim()+":"+tcpPort); note("Connected. Sign in or create an account.");
        }));
    }
    private void authenticate(boolean creating) {
        if(!client.isConnected()) { note("Connect to the platform first."); return; }
        String user=username.getText().trim(); char[] secret=password.getPassword(); String secretText=new String(secret); Arrays.fill(secret,'\0');
        login.setEnabled(false); register.setEnabled(false); note(creating?"Creating account…":"Signing in…");
        client.request(creating?"register":"login",Map.of("username",user,"password",secretText)).whenComplete((response,error)->onUI(()-> {
            login.setEnabled(true); register.setEnabled(true); password.setText("");
            if(error!=null) { showError(error); return; }
            if(creating) { note("Account created. Enter your password again to sign in."); return; }
            authenticated=true; accountLabel.setText("Signed in as "+response.path("username").asText(user));
            pages.show(root,"dashboard"); note("Authenticated. Monitoring connects only through the platform."); refresh(); refreshTimer.start();
        }));
    }
    private JPanel dashboard() {
        JPanel page=panel(new BorderLayout(0,18)); page.setBorder(new EmptyBorder(24,26,20,26));
        JPanel top=panel(new BorderLayout()); JPanel title=panel(new GridLayout(2,1,0,8)); title.add(heading("Operations workspace",27)); title.add(accountLabel); top.add(title);
        JButton logout=new JButton("Sign out"); logout.addActionListener(event->receive(client.request("logout",Map.of()),value->{
            authenticated=false; refreshTimer.stop(); pages.show(root,"auth"); note("Signed out."); })); top.add(logout,BorderLayout.EAST); page.add(top,BorderLayout.NORTH);
        JTabbedPane tabs=new JTabbedPane(); tabs.addTab("Health monitoring",monitoringPage()); tabs.addTab("Command console",consolePage()); page.add(tabs); return page;
    }
    private JPanel monitoringPage() {
        JPanel page=panel(new BorderLayout(0,14)); page.setBorder(new EmptyBorder(18,0,0,0));
        JPanel toolbar=panel(new BorderLayout()); toolbar.add(overviewStatus);
        JPanel buttons=panel(new FlowLayout(FlowLayout.RIGHT,8,0)); JButton refresh=new JButton("Refresh"), details=new JButton("Open details");
        refresh.addActionListener(event->refresh()); details.addActionListener(event->openSelected()); buttons.add(refresh); buttons.add(details); toolbar.add(buttons,BorderLayout.EAST); page.add(toolbar,BorderLayout.NORTH);
        jvms.setDefaultRenderer(Object.class,new DefaultTableCellRenderer(){
            @Override public Component getTableCellRendererComponent(JTable table,Object value,boolean selected,boolean focus,int row,int col) {
                Component cell=super.getTableCellRendererComponent(table,value,selected,focus,row,col);
                if(!selected) { cell.setBackground(row%2==0?Color.WHITE:new Color(247,249,252));
                    String text=String.valueOf(value); cell.setForeground(text.equals("HEALTHY")||text.equals("CONNECTED")?new Color(20,126,88):
                        text.equals("UNHEALTHY")||text.equals("DISCONNECTED")||text.equals("RECOVERY_FAILED")?new Color(178,47,58):new Color(37,52,72)); }
                setBorder(new EmptyBorder(0,10,0,10)); return cell;
            }});
        jvms.addMouseListener(new MouseAdapter(){@Override public void mouseClicked(MouseEvent event){if(event.getClickCount()==2)openSelected();}});
        page.add(new JScrollPane(jvms));
        JPanel bottom=panel(new BorderLayout()); bottom.add(new JLabel("Double-click a JVM for TCP connections, JVM arguments, heap pools and GC."));
        JPanel actions=panel(new FlowLayout(FlowLayout.RIGHT));
        for(String action:new String[]{"start","stop","restart"}) { JButton button=new JButton(action.substring(0,1).toUpperCase()+action.substring(1)+" EC");
            button.addActionListener(event->ecAction(action)); actions.add(button); }
        bottom.add(actions,BorderLayout.EAST); page.add(bottom,BorderLayout.SOUTH); return page;
    }
    private JPanel consolePage() {
        JPanel page=panel(new BorderLayout(0,12)); page.setBorder(new EmptyBorder(18,0,0,0));
        console.setText("Platform command console\nType mng help to inspect available commands.\n"); page.add(new JScrollPane(console));
        JPanel input=panel(new BorderLayout(10,0)); JButton run=new JButton("Run"); input.add(command); input.add(run,BorderLayout.EAST); page.add(input,BorderLayout.SOUTH);
        Runnable execute=()->runCommand(command.getText().trim()); run.addActionListener(event->execute.run()); command.addActionListener(event->execute.run()); return page;
    }
    private void runCommand(String text) {
        if (text.matches("(?i)mng\\s+(login|register|session).*")) {
            note("Use the sign-in screen for account credentials."); return;
        }
        console.append("\n> "+text+"\n"); receive(client.request("command",Map.of("command",text)),response->{console.append(response.path("body").asText()+"\n");note("Command completed.");});
    }
    private String selectedId() { int row=jvms.getSelectedRow(); return row<0?null:String.valueOf(monitoring.getValueAt(jvms.convertRowIndexToModel(row),0)); }
    private void ecAction(String action) {
        int row=jvms.getSelectedRow(); if(row<0) { note("Select an EC first.");return; }
        int modelRow=jvms.convertRowIndexToModel(row); String id=String.valueOf(monitoring.getValueAt(modelRow,0));
        if(!"EC".equals(monitoring.getValueAt(modelRow,1))) { note("Lifecycle actions apply to EC rows only.");return; }
        if(!action.equals("start")&&JOptionPane.showConfirmDialog(frame,action+" "+id+"?","Confirm EC operation",JOptionPane.OK_CANCEL_OPTION)!=JOptionPane.OK_OPTION)return;
        note("Requesting "+action+" for "+id+"…"); receive(client.request("command",Map.of("command","mng "+action+" ec "+id)),response->{note(response.path("body").asText());console.append(response.path("body").asText()+"\n");refresh();});
    }
    private void refresh() {
        if(!authenticated||refreshing||!client.isConnected())return;
        refreshing=true; overviewStatus.setText("Updating live JVM snapshots…");
        client.request("overview",Map.of()).whenComplete((response,error)->onUI(()->{
            refreshing=false; if(error!=null){overviewStatus.setText("Monitoring update failed");showError(error);return;}
            String selected=selectedId(); monitoring.setRowCount(0); int connected=0;
            for(JsonNode row:response.path("jvms")) {
                boolean linked=row.path("connected").asBoolean(); if(linked)connected++;
                JsonNode machine=row.path("machine"); long total=machine.path("memoryTotal").asLong(-1), free=machine.path("memoryFree").asLong(-1);
                monitoring.addRow(new Object[]{row.path("id").asText(),row.path("type").asText(),linked?"CONNECTED":"DISCONNECTED",row.path("status").asText(),
                    row.path("pid").asText("—"),percent(machine.path("cpuLoad").asDouble(-1)),percent(machine.path("processCpuLoad").asDouble(-1)),
                    total>0?percent((double)(total-free)/total):"N/A",row.has("heapUsed")?bytes(row.path("heapUsed").asLong())+" / "+bytes(row.path("heapMax").asLong()):"N/A",
                    row.path("registries").has("correlations")?row.path("registries").path("correlations").asText()+" / "+row.path("registries").path("correlationLimit").asText():"N/A"});
            }
            if(selected!=null)for(int index=0;index<monitoring.getRowCount();index++)if(selected.equals(monitoring.getValueAt(index,0))){int view=jvms.convertRowIndexToView(index);jvms.setRowSelectionInterval(view,view);break;}
            overviewStatus.setText(monitoring.getRowCount()+" JVMs  •  "+connected+" connected  •  Updated "+java.time.LocalTime.now().withNano(0));
        }));
    }
    private static String bytes(long value) { return value<0?"N/A":String.format(Locale.ROOT,"%.1f MiB",value/1048576.0); }
    private static String percent(double value) { return value<0?"N/A":String.format(Locale.ROOT,"%.1f%%",100*value); }
    private void openSelected() {
        String target=selectedId(); if(target==null){note("Select a JVM first.");return;}
        new DetailWindow(target).open();
    }
    private final class DetailWindow {
        private final String target;
        private final JDialog dialog=new JDialog(frame,"JVM detail",false);
        private final JLabel summary=new JLabel("Loading…");
        private final DefaultTableModel heaps=model("Memory pool","Used","Committed","Maximum");
        private final DefaultTableModel connections=model("Service","Local address","Remote address","State");
        private final JTextArea arguments=area(), runtime=area(), serviceLoad=area();
        private final JButton gc=new JButton("Run GC"), refresh=new JButton("Refresh");
        private boolean busy;
        DetailWindow(String target){this.target=target;}
        void open(){
            dialog.setTitle(target+" • JVM details"); dialog.setSize(960,660); dialog.setLocationRelativeTo(frame);
            JPanel page=panel(new BorderLayout(0,16));page.setBorder(new EmptyBorder(22,24,22,24));
            JPanel top=panel(new BorderLayout());top.add(heading(target,25)); JPanel actions=panel(new FlowLayout(FlowLayout.RIGHT));actions.add(refresh);actions.add(gc);top.add(actions,BorderLayout.EAST);
            JPanel header=panel(new BorderLayout(0,10));header.add(top,BorderLayout.NORTH);header.add(summary);page.add(header,BorderLayout.NORTH);
            JTabbedPane tabs=new JTabbedPane(); tabs.addTab("Heap statistics",new JScrollPane(table(heaps)));tabs.addTab("TCP connections",new JScrollPane(table(connections)));
            tabs.addTab("JVM arguments",new JScrollPane(arguments));tabs.addTab("Machine & runtime",new JScrollPane(runtime));
            tabs.addTab("Service load",new JScrollPane(serviceLoad));page.add(tabs);dialog.add(page);
            refresh.addActionListener(event->load(false)); gc.addActionListener(event->{if(JOptionPane.showConfirmDialog(dialog,"Request garbage collection in "+target+"? The JVM may ignore this request.","Run GC",JOptionPane.OK_CANCEL_OPTION)==JOptionPane.OK_OPTION)load(true);});
            dialog.setVisible(true);load(false);
        }
        void load(boolean runGc){
            if(busy)return;busy=true;gc.setEnabled(false);refresh.setEnabled(false);summary.setText(runGc?"Requesting GC…":"Refreshing diagnostics…");
            client.request(runGc?"gc":"details",Map.of("target",target)).whenComplete((response,error)->onUI(()->{
                busy=false;gc.setEnabled(true);refresh.setEnabled(true);if(!dialog.isDisplayable())return;
                if(error!=null){summary.setText("Diagnostics unavailable");showError(error);return;}
                JsonNode data=response.path("details");summary.setText(data.path("status").asText()+"  •  PID "+data.path("pid").asText()+"  •  "+data.path("jvmName").asText()+"  •  Java "+data.path("javaVersion").asText());
                heaps.setRowCount(0);memoryRow("Heap total",data.path("heap"));memoryRow("Non-heap total",data.path("nonHeap"));
                for(JsonNode pool:data.path("memoryPools"))memoryRow(pool.path("name").asText(),pool);
                connections.setRowCount(0);for(JsonNode row:data.path("connections"))connections.addRow(new Object[]{row.path("service").asText(),row.path("local").asText(),row.path("remote").asText(),row.path("state").asText()});
                StringBuilder text=new StringBuilder();for(JsonNode argument:data.path("jvmArguments"))text.append(argument.asText()).append('\n');arguments.setText(text.toString());arguments.setCaretPosition(0);
                runtime.setText("MACHINE LOAD\n"+data.path("machine").toPrettyString()+"\n\nREGISTRY CAPACITY\n"+data.path("registries").toPrettyString()+"\n\nGARBAGE COLLECTORS\n"+data.path("gc").toPrettyString());runtime.setCaretPosition(0);
                serviceLoad.setText(data.path("services").toPrettyString());serviceLoad.setCaretPosition(0);
                if(runGc)note(response.path("body").asText());
            }));
        }
        void memoryRow(String name,JsonNode usage){heaps.addRow(new Object[]{name,bytes(usage.path("used").asLong(-1)),bytes(usage.path("committed").asLong(-1)),bytes(usage.path("max").asLong(-1))});}
    }
    public static void main(String[] args) throws Exception {
        if(Arrays.asList(args).contains("--headless-check")) {
            if(!SwingUtilities.isEventDispatchThread())SwingUtilities.invokeAndWait(()->{assert table(model("Test")).getRowCount()==0;});
            System.out.println("Swing components initialized successfully; desktop connects only to the configured platform endpoint.");return;
        }
        for(UIManager.LookAndFeelInfo info:UIManager.getInstalledLookAndFeels())if(info.getName().equals("Nimbus")){UIManager.setLookAndFeel(info.getClassName());break;}
        UIManager.put("defaultFont",new Font(Font.SANS_SERIF,Font.PLAIN,13));
        SwingUtilities.invokeLater(DesktopApplication::new);
    }
}
