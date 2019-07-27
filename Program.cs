using System;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Text;
using System.Threading;
using System.Timers;

namespace WindowsRules
{

    class MSGBus
    {
        public const int RULES_TIMER_MAX = 8;
        public const int USER_VAR_MAX = 64;
        public const int USER_STRING_VAR_MAX = 64;
        public const int CONFIRM_QUEUE_MAX = 8;
        public const int UNIT_MAX = 32;
        public const String FILE_BOOT = "boot.txt";
        public const String FILE_RULES = "rules.txt";

        public struct SettingsStruct
        {
            public int Port;
            public String Name;
            public String Group;
            public int webPort;
        }
        private static SettingsStruct Settings;

        public struct NodeStruct
        {
            public Byte[] IP;
            public Byte age;
            public String nodeName;
            public String group;
        }
        private static NodeStruct[] Nodes = new NodeStruct[UNIT_MAX];

        public struct nvarStruct
        {
            public String Name;
            public float Value;
            public byte Decimals;
        }
        private static nvarStruct[] nUserVar = new nvarStruct[265];

        public struct svarStruct
        {
            public String Name;
            public String Value;
        }
        public static svarStruct[] sUserVar = new svarStruct[265];

        public struct timerStruct
        {
            public String Name;
            public Int32 Value;
        }
        public static timerStruct[] RulesTimer = new timerStruct[RULES_TIMER_MAX];

        public struct confirmQueueStruct
        {
            public String Name;
            public byte Attempts;
            public byte State;
            public byte TimerTicks;
        }
        public static confirmQueueStruct[] confirmQueue = new confirmQueueStruct[8];

        public static UdpClient client;
        public static IPEndPoint receivePoint;

        private static System.Timers.Timer timerTenPerSecond;
        private static System.Timers.Timer timerEachSecond;
        private static System.Timers.Timer timerEachMinute;

        private static TcpListener webListener;

        public static byte[] sortedIndex = new byte[UNIT_MAX + 1];

        public static String printWebString;

        static void Main(string[] args)
        {
            Settings.Name = "Windows_NoName";
            Settings.Port = 65501;
            Settings.webPort = 80;

            // init nodestruct
            for (byte x = 0; x < UNIT_MAX; x++)
            {
                Nodes[x].IP = new byte[4];
                Nodes[x].IP[0] = 0;
            }

            // Create an empty Boot file if it does not exist
            FileInfo boot = new FileInfo(FILE_BOOT);
            if (!boot.Exists)
                File.Create(FILE_BOOT).Dispose();

            // Create an empty Rules file if it does not exist
            FileInfo rules = new FileInfo(FILE_RULES);
            if (!rules.Exists)
                File.Create(FILE_RULES).Dispose();

            rulesProcessing("", FILE_BOOT, "System#Config");

            // Setup UDP network IP
            IPAddress LocalIP;
            IPEndPoint localIpEndPoint;

            // Autodetermine local IP to use, or override using commandline options
            if (args.Length != 2)
            {
                string PublicIP = "8.8.8.8";
                UdpClient check = new UdpClient(PublicIP, 1);
                LocalIP = ((IPEndPoint)check.Client.LocalEndPoint).Address;
                log("Using IP:" + LocalIP.ToString() + " on port " + Settings.Port);
            }
            else
            {
                LocalIP = IPAddress.Parse(args[0]);
                Settings.Port = Convert.ToInt32(args[1]);
            }

            // Setup UDP client socket
            localIpEndPoint = new IPEndPoint(LocalIP, Settings.Port);
            client = new UdpClient(localIpEndPoint);
            receivePoint = new IPEndPoint(new IPAddress(0), 0);

            // Launch a thread to handle incoming UDP packets
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                handleUDP();
            }).Start();

            // Setup a tiny webserver
            webListener = new TcpListener(LocalIP, Settings.webPort);
            webListener.Start();
            Thread web = new Thread(new ThreadStart(webServer));
            web.Start();


            // Launch a timer to handle tasks 10 times per second
            timerTenPerSecond = new System.Timers.Timer(100);
            timerTenPerSecond.Elapsed += timerOnTenPerSecond;
            timerTenPerSecond.AutoReset = true;
            timerTenPerSecond.Enabled = true;

            // Launch a timer to handle tasks once each second
            timerEachSecond = new System.Timers.Timer(1000);
            timerEachSecond.Elapsed += timerOnEachSecond;
            timerEachSecond.AutoReset = true;
            timerEachSecond.Enabled = true;

            // Launch a timer to handle tasks once each minute
            timerEachMinute = new System.Timers.Timer(60000);
            timerEachMinute.Elapsed += timerOnEachMinute;
            timerEachMinute.AutoReset = true;
            timerEachMinute.Enabled = true;

            // Get others to announce themselves so i can update my node list
            UDPSend("MSGBUS/Refresh");

            // Run boot event
            rulesProcessing("", FILE_BOOT, "System#Boot");
            rulesProcessing("", FILE_RULES, "System#Boot");

            MSGBusAnnounceMe();

            while (true)
            {
                //Console.WriteLine("MainLoop");
                System.Threading.Thread.Sleep(1000);
            }
        }


        // Handle Stuff 10 times per second
        private static void timerOnTenPerSecond(Object source, ElapsedEventArgs e)
        {
            UDPQueue();
        }

        // Handle Stuff each second
        private static void timerOnEachSecond(Object source, ElapsedEventArgs e)
        {
            rulesTimers();
        }

        // Handle Stuff each minute
        private static void timerOnEachMinute(Object source, ElapsedEventArgs e)
        {
            MSGBusRefreshNodeList();
            MSGBusAnnounceMe();
        }

        private static void rulesTimers()
        {
            for (byte x = 0; x < RULES_TIMER_MAX; x++)
            {
                if (RulesTimer[x].Value != 0L) // timer active?
                {
                    RulesTimer[x].Value--;
                    if (RulesTimer[x].Value == 0) // timer finished?
                    {
                        String strEvent = "Timer#";
                        strEvent += RulesTimer[x].Name;
                        rulesProcessing("", FILE_RULES, strEvent);
                    }
                }
            }
        }


        private static void UDPQueue()
        {
            for (byte x = 0; x < CONFIRM_QUEUE_MAX; x++)
            {
                if (confirmQueue[x].State == 1)
                {
                    if (confirmQueue[x].Attempts != 0)
                    {
                        confirmQueue[x].TimerTicks--;
                        if (confirmQueue[x].TimerTicks == 0)
                        {
                            confirmQueue[x].TimerTicks = 3;
                            confirmQueue[x].Attempts--;
                            UDPSend(confirmQueue[x].Name);
                        }
                    }
                    else
                    {
                        log("Confirmation Timeout");
                        confirmQueue[x].State = 0;
                    }
                }
            }
        }

        // Handle incoming MSGBus messages
        static void handleUDP()
        {
            while (true)
            {
                Byte[] received = client.Receive(ref receivePoint);
                String strdata = System.Text.Encoding.ASCII.GetString(received);
                if (strdata.Length > 0)
                    ProcData(receivePoint, strdata);
            }
        }


        // Process incoming MSGBus messages
        static void ProcData(IPEndPoint RemoteHost, String msg)
        {
            msg = msg.Replace("\r", "");
            msg = msg.Replace("\n", "");
            msg = msg.Replace("\0", "");

            // First process messages that request confirmation
            // These messages start with '>' and must be addressed to my node name
            String mustConfirm = ">" + "WIN" + "/";
            if (msg.StartsWith(mustConfirm))
            {
                String reply = "<" + msg.Substring(1);
                UDPSend(reply);
            }
            if (msg[0] == '>')
            {
                msg = msg.Substring(1); // Strip the '>' request token from the message
            }

            // Process confirmation messages
            if (msg[0] == '<')
            {
                for (byte x = 0; x < CONFIRM_QUEUE_MAX; x++) // todo
                {
                    if (confirmQueue[x].Name != null)
                    {
                        if (confirmQueue[x].Name.Substring(1) == msg.Substring(1))
                        {
                            confirmQueue[x].State = 0;
                            break;
                        }
                    }
                }
                log("UDP: " + msg);
                return; // This message needs no further processing, so return.
            }

            // Special MSGBus system events
            if (msg.Length >= 7 && msg.Substring(0, 7) == "MSGBUS/")
            {
                String sysMSG = msg.Substring(7);
                if (sysMSG.Length > 9 && sysMSG.Substring(0, 9) == "Hostname=")
                {
                    String parameters = sysMSG.Substring(9);
                    String hostName = parseString(parameters, 1);
                    //String ip = parseString(params, 2); we just take the remote ip here
                    String group = parseString(parameters, 3);
                    MSGBusNodelist(RemoteHost, hostName, group);
                }
                if (sysMSG.Length == 7 && sysMSG.Substring(0, 7) == "Refresh")
                {
                    MSGBusAnnounceMe();
                }
            }

            //String strLog = "UDP: " + msg;
            //log(strLog);
            rulesProcessing(RemoteHost.Address.ToString(), FILE_RULES, msg);
        }

        static void MSGBusAnnounceMe()
        {
            String msg = "MSGBUS/Hostname=";
            msg += Settings.Name;
            msg += ",0.0.0.0,";
            msg += Settings.Group;

            UDPSend(msg);
        }

        static void MSGBusNodelist(IPEndPoint RemoteHost, String msg, String group)
        {
            byte[] remoteIP = RemoteHost.Address.GetAddressBytes();
            if (group.Length == 0)
                group = "-";

            Boolean found = false;
            for (byte x = 0; x < UNIT_MAX; x++)
            {
                if (Nodes[x].nodeName == msg)
                {
                    Nodes[x].group = group;
                    for (byte y = 0; y < 4; y++)
                        Nodes[x].IP[y] = remoteIP[y];
                    Nodes[x].age = 0;
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                for (byte x = 0; x < UNIT_MAX; x++)
                {
                    if (Nodes[x].IP[0] == 0)
                    {
                        Nodes[x].nodeName = msg;
                        Nodes[x].group = group;
                        for (byte y = 0; y < 4; y++)
                            Nodes[x].IP[y] = remoteIP[y];
                        Nodes[x].age = 0;
                        break;
                    }
                }
            }
        }

        static void MSGBusRefreshNodeList()
        {
            // start at 1, 0 = myself and does not age...
            for (byte counter = 1; counter < UNIT_MAX; counter++)
            {
                if (Nodes[counter].IP[0] != 0)
                {
                    Nodes[counter].age++;  // increment age counter
                    if (Nodes[counter].age > 10) // if entry to old, clear this node ip from the list.
                        for (byte x = 0; x < 4; x++)
                            Nodes[counter].IP[x] = 0;
                }
            }
        }

        static void UDPSend(String msg)
        {
            UdpClient client = new UdpClient();
            IPAddress Address = IPAddress.Parse("255.255.255.255");
            IPEndPoint Endpoint = new IPEndPoint(Address, Settings.Port);
            byte[] bytes = Encoding.ASCII.GetBytes(msg);
            client.Send(bytes, bytes.Length, Endpoint);
            client.Close();
        }

        // Rule engine
        static String rulesProcessing(String RemoteHost, String fileName, String strEvent)
        {
            String strLog = "EVT: ";
            strLog += strEvent;
            log(strLog);

            String f = File.ReadAllText(fileName);
            String[] arrLines = f.Split('\n');
            // todo static byte ...
            byte nestingLevel = 0;

            nestingLevel++;
            if (nestingLevel > 3)
            {
                strLog = "EVENT: Error: Nesting level exceeded!";
                nestingLevel--;
                return (strLog);
            }

            Boolean match = false;
            Boolean codeBlock = false;
            Boolean isCommand = false;
            Boolean conditional = false;
            Boolean condition = false;
            Boolean ifBranche = false;

            foreach (String inputLine in arrLines)
            {
                String line = inputLine.Replace("\r", "");
                if (line.Length > 0 && line.Substring(0, 2) != "//")
                {
                    isCommand = true;

                    int comment = line.IndexOf(" //");
                    if (comment > 0)
                        line = line.Substring(0, comment);

                    if (match || !codeBlock)
                    {
                        line = parseTemplate(line, line.Length);
                    }
                    line = line.Trim();

                    String lineOrg = line; // store original line for future use
                    line = line.ToLower(); // convert all to lower case to make checks easier


                    String eventTrigger = "";
                    String action = "";

                    if (!codeBlock)  // do not check "on" rules if a block of actions is to be processed
                    {
                        if (line.StartsWith("on "))
                        {
                            line = line.Substring(3);
                            int split = line.IndexOf(" do");
                            if (split != -1)
                            {
                                eventTrigger = line.Substring(0, split);
                                if (split + 7 < lineOrg.Length)
                                {
                                    action = lineOrg.Substring(split + 7);
                                    action.Trim();
                                }
                            }
                            if (eventTrigger == "*") // wildcard, always process
                                match = true;
                            else
                                match = ruleMatch(strEvent, eventTrigger);
                            if (action.Length > 0) // single on/do/action line, no block
                            {
                                isCommand = true;
                                codeBlock = false;
                            }
                            else
                            {
                                isCommand = false;
                                codeBlock = true;
                            }
                        }
                    }
                    else
                    {
                        action = lineOrg;
                    }

                    String lcAction = action;
                    lcAction = lcAction.ToLower();
                    if (lcAction == "endon") // Check if action block has ended, then we will wait for a new "on" rule
                    {
                        isCommand = false;
                        codeBlock = false;
                        match = false;
                    }

                    if (match) // rule matched for one action or a block of actions
                    {
                        int split = lcAction.IndexOf("if "); // check for optional "if" condition
                        if (split != -1)
                        {
                            conditional = true;
                            String check = lcAction.Substring(split + 3);
                            condition = conditionMatchExtended(check);
                            ifBranche = true;
                            isCommand = false;
                        }

                        if (lcAction == "else") // in case of an "else" block of actions, set ifBranche to false
                        {
                            ifBranche = false;
                            isCommand = false;
                        }

                        if (lcAction == "endif") // conditional block ends here
                        {
                            conditional = false;
                            isCommand = false;
                        }

                        // process the action if it's a command and unconditional, or conditional and the condition matches the if or else block.
                        if (isCommand && ((!conditional) || (conditional && (condition == ifBranche))))
                        {
                            if (strEvent[0] == '!')
                            {
                                action = action.Replace("%eventvalue%", strEvent); // substitute %eventvalue% with literal event string if starting with '!'
                            }
                            else
                            {
                                int equalsPos = strEvent.IndexOf("=");
                                if (equalsPos > 0)
                                {
                                    String tmpString = strEvent.Substring(equalsPos + 1);
                                    action = action.Replace("%eventvalue%", tmpString); // substitute %eventvalue% in actions with the actual value from the event
                                    action = action.Replace("%event%", strEvent); // substitute %event% with literal event string
                                }
                            }

                            strLog = "ACT: ";
                            strLog += action;
                            log(strLog);
                            ExecuteCommand(RemoteHost, action);
                        }
                    }

                }
            }
            nestingLevel--;
            return "";
        }


        // Rule Engine, match rule against event
        static Boolean ruleMatch(String strEvent, String rule)
        {

            Boolean match = false;
            String tmpEvent = strEvent;
            String tmpRule = rule;

            // Special handling of literal string events, they should start with '!'
            if (strEvent[0] == '!')
            {
                int hashPos = rule.IndexOf('#');
                if (hashPos == -1) // no # sign in rule, use 'wildcard' match on event 'source'
                {
                    tmpEvent = strEvent.Substring(0, rule.Length);
                    tmpRule = rule;
                }
                int asteriskPos = rule.IndexOf('*');
                if (asteriskPos != -1) // a * sign in rule, so use a'wildcard' match on message
                {
                    tmpEvent = strEvent.Substring(0, asteriskPos - 1);
                    tmpRule = rule.Substring(0, asteriskPos - 1);
                }

                if (tmpEvent.Equals(tmpRule, StringComparison.OrdinalIgnoreCase))
                    return true;
                else
                    return false;
            }
            /*
            todo if needed

              if (event.startsWith("Clock#Time")) // clock events need different handling...
              {
                int pos1 = event.indexOf("=");
                int pos2 = rule.indexOf("=");
                if (pos1 > 0 && pos2 > 0)
                {
                  tmpEvent = event.substring(0, pos1);
                  tmpRule  = rule.substring(0, pos2);
                  if (tmpRule.equalsIgnoreCase(tmpEvent)) // if this is a clock rule
                  {
                    tmpEvent = event.substring(pos1 + 1);
                    tmpRule  = rule.substring(pos2 + 1);
                    unsigned long clockEvent = string2TimeLong(tmpEvent);
                    unsigned long clockSet = string2TimeLong(tmpRule);
                    if (matchClockEvent(clockEvent, clockSet))
                      return true;
                    else
                      return false;
                  }
                }
              }
            */

            // parse event into verb and value
            float value = 0;
            int equalsPos = strEvent.IndexOf("=");
            if (equalsPos != -1)
            {
                tmpEvent = strEvent.Substring(equalsPos + 1);
                try
                {
                    value = float.Parse(tmpEvent);
                }
                catch { }
                tmpEvent = strEvent.Substring(0, equalsPos);
            }

            // parse rule
            int comparePos = 0;
            char compare = ' ';
            comparePos = rule.IndexOf(">");
            if (comparePos > 0)
            {
                compare = '>';
            }
            else
            {
                comparePos = rule.IndexOf("<");
                if (comparePos > 0)
                {
                    compare = '<';
                }
                else
                {
                    comparePos = rule.IndexOf("=");
                    if (comparePos > 0)
                    {
                        compare = '=';
                    }
                }
            }

            float ruleValue = 0;

            if (comparePos > 0)
            {
                tmpRule = rule.Substring(comparePos + 1);
                ruleValue = float.Parse(tmpRule);
                tmpRule = rule.Substring(0, comparePos);
            }

            switch (compare)
            {
                case '>':
                    if (tmpRule.Equals(tmpEvent, StringComparison.OrdinalIgnoreCase) && value > ruleValue)
                        match = true;
                    break;

                case '<':
                    if (tmpRule.Equals(tmpEvent, StringComparison.OrdinalIgnoreCase) && value < ruleValue)
                        match = true;
                    break;

                case '=':
                    if (tmpRule.Equals(tmpEvent, StringComparison.OrdinalIgnoreCase) && value == ruleValue)
                        match = true;
                    break;

                case ' ':
                    if (tmpRule.Equals(tmpEvent, StringComparison.OrdinalIgnoreCase))
                        match = true;
                    break;
            }
            return match;
        }


        // Check Rule conditions for AND/OR
        static Boolean conditionMatchExtended(String check)
        {
            int condAnd = -1;
            int condOr = -1;
            Boolean rightcond = false;
            Boolean leftcond = conditionMatch(check); // initial check

            do
            {
                condAnd = check.IndexOf(" and ");
                condOr = check.IndexOf(" or ");

                if (condAnd > 0 || condOr > 0)
                { // we got AND/OR
                    if (condAnd > 0 && ((condOr < 0 && condOr < condAnd) || (condOr > 0 && condOr > condAnd)))
                    { //AND is first
                        check = check.Substring(condAnd + 5);
                        rightcond = conditionMatch(check);
                        leftcond = (leftcond && rightcond);
                    }
                    else
                    { //OR is first
                        check = check.Substring(condOr + 4);
                        rightcond = conditionMatch(check);
                        leftcond = (leftcond || rightcond);
                    }
                }
            } while (condAnd > 0 || condOr > 0);
            return leftcond;
        }


        // Check Rule conditions
        static Boolean conditionMatch(String check)
        {
            Boolean match = false;

            char compare = ' ';

            int posStart = check.Length;
            int posEnd = posStart;
            int comparePos = 0;

            if ((comparePos = check.IndexOf("!=")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 2;
                //compare = '!' + '=';
            }
            if ((comparePos = check.IndexOf("<>")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 2;
                //compare = '!' + '=';
            }
            if ((comparePos = check.IndexOf(">=")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 2;
                //compare = '>' + '=';
            }
            if ((comparePos = check.IndexOf("<=")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 2;
                //compare = '<' + '=';
            }
            if ((comparePos = check.IndexOf("<")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 1;
                compare = '<';
            }
            if ((comparePos = check.IndexOf(">")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 1;
                compare = '>';
            }
            if ((comparePos = check.IndexOf("=")) > 0 && comparePos < posStart)
            {
                posStart = comparePos;
                posEnd = posStart + 1;
                compare = '=';
            }

            float Value1 = 0;
            float Value2 = 0;

            if (compare > ' ')
            {
                String tmpCheck1 = check.Substring(0, posStart);
                String tmpCheck2 = check.Substring(posEnd);
                // todo if (!isFloat(tmpCheck1) || !isFloat(tmpCheck2)) {
                //    Value1 = timeStringToSeconds(tmpCheck1);
                //    Value2 = timeStringToSeconds(tmpCheck2);
                //} else {
                Value1 = float.Parse(tmpCheck1);
                Value2 = float.Parse(tmpCheck2);
                //}
            }
            else
                return false;

            switch (compare)
            {
                /* todo
                case '>' + '=':
                    if (Value1 >= Value2)
                        match = true;
                    break;

                case '<' + '=':
                    if (Value1 <= Value2)
                        match = true;
                    break;

                case '!' + '=':
                    if (Value1 != Value2)
                        match = true;
                    break;
*/
                case '>':
                    if (Value1 > Value2)
                        match = true;
                    break;

                case '<':
                    if (Value1 < Value2)
                        match = true;
                    break;

                case '=':
                    if (Value1 == Value2)
                        match = true;
                    break;
            }
            return match;
        }


        // Rule engine, parse incoming events before processing
        static String parseTemplate(String tmpString, int lineSize)
        {
            String newString = tmpString;

            // check named uservars
            for (int x = 0; x < USER_VAR_MAX; x++)
            {
                String varname = "%" + nUserVar[x].Name + "%";
                String decFormat = "n" + nUserVar[x].Decimals;
                String svalue = nUserVar[x].Value.ToString(decFormat, System.Globalization.CultureInfo.InvariantCulture); //, nUserVar[x].Decimals);
                newString = newString.Replace(varname, svalue);
            }

            // check named uservar strings
            for (int x = 0; x < USER_STRING_VAR_MAX; x++)
            {
                String varname = "%" + sUserVar[x].Name + "%";
                String svalue = sUserVar[x].Value;
                newString = newString.Replace(varname, svalue);
            }
            return newString;
        }


        // Rule engine, set numerical variables
        static void setNvar(String varName, float value, int decimals)
        {
            int pos = -1;
            for (int x = 0; x < USER_VAR_MAX; x++)
            {
                if (nUserVar[x].Name == varName)
                {
                    nUserVar[x].Value = value;
                    if (decimals != -1)
                        nUserVar[x].Decimals = (byte)decimals;
                    return;
                }
                if (pos == -1 && nUserVar[x].Name == null)
                    pos = x;
            }
            if (pos != -1)
            {
                nUserVar[pos].Name = varName;
                nUserVar[pos].Value = value;
                if (decimals != -1)
                    nUserVar[pos].Decimals = (byte)decimals;
                else
                    nUserVar[pos].Decimals = 2;
            }
        }


        // Rule engine, set string variables
        static void setSvar(String varName, String value)
        {
            int pos = -1;
            for (int x = 0; x < USER_STRING_VAR_MAX; x++)
            {
                if (sUserVar[x].Name == varName)
                {
                    sUserVar[x].Value = value;
                    return;
                }
                if (pos == -1 && sUserVar[x].Name == null)
                    pos = x;
            }
            if (pos != -1)
            {
                sUserVar[pos].Name = varName;
                sUserVar[pos].Value = value;
            }
        }


        // Rule engine, set timers
        static void setTimer(String varName, int value)
        {
            int pos = -1;
            for (byte x = 0; x < RULES_TIMER_MAX; x++)
            {
                if (RulesTimer[x].Name == varName)
                {
                    RulesTimer[x].Value = value;
                    return;
                }
                if (pos == -1 && RulesTimer[x].Name == null)
                    pos = x;
            }
            if (pos != -1)
            {
                RulesTimer[pos].Name = varName;
                RulesTimer[pos].Value = value;
            }
        }


        // Rule engine, execute action commands
        static void ExecuteCommand(String RemoteHost, String Line)
        {
            Boolean success = false;
            String command = parseString(Line, 1);

            if (command.Equals("Config", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                String setting = parseString(Line, 2);
                if (setting.Equals("Name", StringComparison.OrdinalIgnoreCase))
                {
                    Settings.Name = parseString(Line, 3);
                }
                if (setting.Equals("Group", StringComparison.OrdinalIgnoreCase))
                {
                    Settings.Group = parseString(Line, 3);
                }
                if (setting.Equals("Port", StringComparison.OrdinalIgnoreCase))
                {
                    Settings.Port = Convert.ToInt32(parseString(Line, 3));
                }
            }

            if (command.Equals("event", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                String sEvent = Line;
                sEvent = sEvent.Substring(6);
                rulesProcessing("", FILE_RULES, sEvent);
            }


            if (command.Equals("valueSet", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                float result = 0;
                // todo Calculate(TmpStr1, &result);
                String varName = parseString(Line, 2);
                String strValue = parseString(Line, 3);
                try
                {
                    result = float.Parse(strValue, System.Globalization.CultureInfo.InvariantCulture);
                }
                catch { }

                setNvar(varName, result, 2);
            }

            if (command.Equals("stringSet", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                String varName = parseString(Line, 2);
                String strValue = parseString(Line, 3);
                setSvar(varName, strValue);
            }

            if (command.Equals("TimerSet", StringComparison.OrdinalIgnoreCase))
            {
                String varName = parseString(Line, 2);
                String strValue = parseString(Line, 3);
                int value = Convert.ToInt32(strValue);
                setTimer(varName, value);
            }

            if (command.Equals("log", StringComparison.OrdinalIgnoreCase))
            {
                String strDumplogfile = "MSGBuslogdump";
                String strMessage = Line.Substring(4);
                String currentdate = DateTime.Now.ToString("yyyy-MM-dd");
                String Now = DateTime.Now.ToString();
                File.AppendAllText(strDumplogfile + "_" + RemoteHost + "_" + currentdate + ".log", Now + " : " + strMessage + "\r\n");
                File.AppendAllText(strDumplogfile + "_" + currentdate + ".log", Now + " " + RemoteHost + " : " + strMessage + "\r\n");
            }

            if (command.Equals("MSGBus", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                String msg = Line;
                msg = msg.Substring(7);
                if (msg[0] == '>')
                {
                    for (byte x = 0; x < CONFIRM_QUEUE_MAX; x++)
                    {
                        if (confirmQueue[x].State == 0)
                        {
                            confirmQueue[x].Name = msg;
                            confirmQueue[x].Attempts = 9;
                            confirmQueue[x].State = 1;
                            confirmQueue[x].TimerTicks = 3;
                            UDPSend(msg);
                            break;
                        }
                    }
                }
                else
                    UDPSend(msg);
            }

            if (command.Equals("SendToUDP", StringComparison.OrdinalIgnoreCase))
            {
                String host = parseString(Line, 2);
                String strPort = parseString(Line, 3);
                int port = Convert.ToInt32(strPort);
                int msgPos = getParamStartPos(Line, 4);
                String strMSG = Line.Substring(msgPos);
                UdpClient client = new UdpClient();
                IPAddress Address = IPAddress.Parse(host);
                IPEndPoint Endpoint = new IPEndPoint(Address, port);
                byte[] bytes = Encoding.ASCII.GetBytes(strMSG);
                client.Send(bytes, bytes.Length, Endpoint);
                client.Close();
            }

            if (command.Equals("SendToHTTP", StringComparison.OrdinalIgnoreCase))
            {
                System.Net.WebClient webClient = new WebClient();
                String host = parseString(Line, 2);
                String port = parseString(Line, 3);
                String path = parseString(Line, 4);
                String url = "http://" + host + ":" + port + path;
                //Console.WriteLine("URL:" + url);
                try
                {
                    webClient.DownloadString(url);
                }
                catch { log("Error connecting!"); }
            }

            if (command.Equals("PutToSSL", StringComparison.OrdinalIgnoreCase))
            {
                String host = parseString(Line, 2);
                String port = parseString(Line, 3);
                String path = parseString(Line, 4);
                String url = "https://" + host + ":" + port + path;
                //Console.WriteLine("URL:" + url);
                int bodyPos = getParamStartPos(Line, 5);
                String strBody = Line.Substring(bodyPos);

                ServicePointManager.ServerCertificateValidationCallback = (sender, cert, chain, sslPolicyErrors) => true;
                System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12;

                System.Net.WebClient webClient = new WebClient();
                byte[] bytes = Encoding.ASCII.GetBytes(strBody);
                try
                {
                    byte[] response = webClient.UploadData(url, "PUT", bytes);
                    string strResponse = webClient.Encoding.GetString(response);
                    log(strResponse);
                }
                catch { log("Error connecting!"); }
            }

            if (command.Equals("webPrint", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                if (Line.Length == 8)
                    printWebString = "";
                else
                    printWebString += Line.Substring(9);
            }

            if (command.Equals("webButton", StringComparison.OrdinalIgnoreCase))
            {
                success = true;
                printWebString += "<a class=\"" + parseString(Line, 2, ';') + "\" href=\"" + parseString(Line, 3, ';') + "\">" + parseString(Line, 4, ';') + "</a>";
            }
        }

        // Rule engine, get positional parameter as string
        static String parseString(String str, byte indexFind, char separator = ',')
        {
            String tmpString = str;
            tmpString += separator;
            tmpString = tmpString.Replace(" ", separator.ToString());
            String locateString = "";
            byte count = 0;
            int index = tmpString.IndexOf(separator);
            while (index > 0)
            {
                count++;
                locateString = tmpString.Substring(0, index);
                tmpString = tmpString.Substring(index + 1);
                index = tmpString.IndexOf(separator);
                if (count == indexFind)
                {
                    return locateString;
                }
            }
            return "";
        }


        // Rule engine, get positional parameter as integer position
        static int getParamStartPos(String str, byte indexFind)
        {
            String tmpString = str;
            byte count = 0;
            tmpString = tmpString.Replace(" ", ",");
            for (int x = 0; x < tmpString.Length; x++)
            {
                if (tmpString[x] == ',')
                {
                    count++;
                    if (count == (indexFind - 1))
                        return x + 1;
                }
            }
            return -1;
        }


        // Generic system log
        static void log(String msg)
        {
            String strLog = DateTime.Now.ToLongTimeString();
            strLog += "." + DateTime.Now.Millisecond;
            strLog += " > " + msg;
            Console.WriteLine(strLog);
        }


        static void webSend(string data, string status, ref Socket webSocket)
        {
            try
            {
                String header = "HTTP/1.1 " + status + "\r\n";
                header += "Content-Type: text/html" + "\r\n";
                header += "Content-Length: " + data.Length + "\r\n\r\n";
                Byte[] hdrdata = Encoding.ASCII.GetBytes(header);
                webSocket.Send(hdrdata, hdrdata.Length, 0);
                Byte[] webdata = Encoding.ASCII.GetBytes(data);
                webSocket.Send(webdata, webdata.Length, 0);
                webSocket.Close();
            }
            catch { };
        }


        static void addHeader(ref String reply)
        {
            reply += "<meta name=\"viewport\" content=\"width=width=device-width, initial-scale=1\">";
            reply += "<STYLE>* {font-family:sans-serif; font-size:12pt;}";
            reply += "h1 {font-size: 16pt; color: #07D; margin: 8px 0; font-weight: bold;}";
            reply += ".button {margin:4px; padding:5px 15px; background-color:#07D; color:#FFF; text-decoration:none; border-radius:4px}";
            reply += ".button-link {padding:5px 15px; background-color:#07D; color:#FFF; border:solid 1px #FFF; text-decoration:none}";
            reply += ".button-widelink {display: inline-block; width: 100%; text-align: center; padding:5px 15px; background-color:#07D; color:#FFF; border:solid 1px #FFF; text-decoration:none}";
            reply += ".button-nodelink {display: inline-block; width: 100%; text-align: center; padding:5px 15px; background-color:#888; color:#FFF; border:solid 1px #FFF; text-decoration:none}";
            reply += ".button-nodelinkA {display: inline-block; width: 100%; text-align: center; padding:5px 15px; background-color:#28C; color:#FFF; border:solid 1px #FFF; text-decoration:none}";
            reply += "td {padding:7px;}";
            reply += "</STYLE>";
            reply += "<a class=\"button-link\" href=\"/\">Main</a>";
            reply += "<a class=\"button-link\" href=\"/boot\">Boot</a>";
            reply += "<a class=\"button-link\" href=\"/rules\">Rules</a>";
            reply += "<a class=\"button-link\" href=\"/tools\">Tools</a>";
            reply += "<BR><BR>";
        }


        static void webServer()
        {

            while (true)
            {
                Socket webSocket = webListener.AcceptSocket();
                if (webSocket.Connected)
                {
                    String webData = "";
                    String webRequest = "";
                    String reply = "<H1>" + Settings.Name + "</H1>";
                    Byte[] data = new Byte[4096];

                    int received = webSocket.Receive(data, data.Length, 0);
                    if (received > 0)
                    {
                        webData = Encoding.ASCII.GetString(data);
                        webData = webData.Substring(0, received);
                        int pos = webData.IndexOf("HTTP", 1);
                        webRequest = webData.Substring(0, pos - 1);
                        log("WEB: " + webRequest);

                        addHeader(ref reply);

                        String path = "";
                        String method = "";
                        if (webData.Substring(0, 3) == "GET")
                        {
                            path = webRequest.Substring(5);
                            method = "GET";
                        }
                        if (webData.Substring(0, 4) == "POST")
                        {
                            path = webRequest.Substring(6);
                            method = "POST";
                        }
                        if (path == "boot")
                            webEdit(FILE_BOOT, method, webData, ref reply);
                        else if (path == "rules")
                            webEdit(FILE_RULES, method, webData, ref reply);
                        else if (path == "tools")
                            webTools(ref reply);
                        else
                            webRoot(path, ref reply);

                    }
                    webSend(reply, "200 OK", ref webSocket);
                }
            }
        }

        static void webRoot(String request, ref String reply)
        {
            String group = "";
            Boolean groupList = true;

            if (request.Length >= 5 && request.Substring(0, 5) == "?cmd=")
            {
                String cmd = request.Substring(5);
                ExecuteCommand("", cmd);
            }
            if (request.Length >= 7 && request.Substring(0, 7) == "?group=")
            {
                group = request.Substring(7);
            }

            if (group != "")
                groupList = false;

            rulesProcessing("", FILE_RULES, "Web#Print");
            reply += printWebString;
            reply += "<form><table>";

            // first get the list in alphabetic order
            for (byte x = 0; x <= UNIT_MAX; x++)
                sortedIndex[x] = x;

            if (groupList == true)
            {
                // Show Group list
                sortDeviceArrayGroup(); // sort on groupname
                String prevGroup = "?";
                for (byte x = 0; x < UNIT_MAX; x++)
                {
                    byte index = sortedIndex[x];
                    if (Nodes[index].IP[0] != 0)
                    {
                        String nodegroup = Nodes[index].group;
                        if (nodegroup != prevGroup)
                        {
                            prevGroup = nodegroup;
                            reply += "<TR><TD><a class=\"";
                            reply += "button-nodelink";
                            reply += "\" ";
                            reply += "href='/?group=";
                            reply += nodegroup;
                            reply += "'>";
                            reply += nodegroup;
                            reply += "</a>";
                            reply += "<TD>";
                        }
                    }
                }
                // All nodes group button
                reply += "<TR><TD><a class=\"button-nodelink\" href='/?group=*'>_ALL_</a><TD>";
            }
            else
            {
                sortDeviceArray();
                for (byte x = 0; x < UNIT_MAX; x++)
                {
                    byte index = sortedIndex[x];
                    if (Nodes[index].IP[0] != 0 && (group == "*" || Nodes[index].group == group))
                    {
                        String buttonclass = "";
                        if (Settings.Name == Nodes[index].nodeName)
                            buttonclass = "button-nodelinkA";
                        else
                            buttonclass = "button-nodelink";
                        reply += "<TR><TD><a class=\"";
                        reply += buttonclass;
                        reply += "\" ";
                        reply += "href='http://";
                        reply += Nodes[index].IP[0];
                        reply += ".";
                        reply += Nodes[index].IP[1];
                        reply += ".";
                        reply += Nodes[index].IP[2];
                        reply += ".";
                        reply += Nodes[index].IP[3];
                        if (group != "")
                        {
                            reply += "?group=";
                            reply += Nodes[index].group;
                        }
                        reply += "'>";
                        reply += Nodes[index].nodeName;
                        reply += "</a>";
                        reply += "<TD>";
                    }
                }
            }
            reply += "</table></form>";
        }

        static void webEdit(String fileName, String method, String Body, ref String reply)
        {

            if (method == "POST")
            {
                String[] bodyLines = Body.Split('\n');
                foreach (String line in bodyLines)
                {
                    if (line.StartsWith("edit="))
                    {
                        String postline = line.Substring(5);
                        postline = postline.Replace("+", " ");
                        postline = Uri.UnescapeDataString(postline);
                        File.WriteAllText(fileName, postline);
                    }
                }
            }

            String f = File.ReadAllText(fileName);
            String[] arrLines = f.Split('\n');
            reply += "<form method='post'>";
            reply += "<textarea name='edit' rows='15' cols='80' wrap='off'>";
            foreach (String line in arrLines)
            {
                String webLine = line;
                webHTMLEscape(ref webLine);
                reply += webLine;
            }
            reply += "</textarea>";
            reply += "<br><input class=\"button-link\" type='submit' value='Submit'>";
            reply += "</form>";
        }


        static void webTools(ref String reply)
        {
            reply += "<form method='post'>";
            reply += "</form>";
        }

        static void webHTMLEscape(ref String html)
        {
            html = html.Replace("&", "&amp;");
            html = html.Replace("\"", "&quot;");
            html = html.Replace("'", "&#039;");
            html = html.Replace("<", "&lt;");
            html = html.Replace(">", "&gt;");
            html = html.Replace("/", "&#047;");
        }

        //********************************************************************************
        // Device Sort routine, switch array entries
        //********************************************************************************
        static void switchArray(int value)
        {
            byte temp;
            temp = sortedIndex[value - 1];
            sortedIndex[value - 1] = sortedIndex[value];
            sortedIndex[value] = temp;
        }


        //********************************************************************************
        // Device Sort routine, compare two array entries
        //********************************************************************************
        static Boolean arrayLessThan(String ptr_1, String ptr_2)
        {
            int i = 0;
            while (i < ptr_1.Length)    // For each character in string 1, starting with the first:
            {
                if (ptr_2.Length < i)    // If string 2 is shorter, then switch them
                {
                    return true;
                }
                else
                {
                    String check1 = ptr_1.Substring(i, 1);  // get the same char from string 1 and string 2
                    String check2 = "";
                    if (i < ptr_2.Length)
                        check2 = ptr_2.Substring(i, 1);
                    if (check1 == check2)
                    {
                        // they're equal so far; check the next char !!
                        i++;
                    }
                    else
                    {
                        char[] c1 = new char[1];
                        c1 = check1.ToCharArray();
                        char[] c2 = new char[1];
                        c2 = check2.ToCharArray();
                        char cc1 = c1[0];
                        char cc2 = c2[0];
                        return (cc2 > cc1);
                    }
                }
            }
            return false;
        }


        //********************************************************************************
        // Device Sort routine, actual sorting
        //********************************************************************************
        static void sortDeviceArray()
        {
            int innerLoop;
            int mainLoop;
            for (mainLoop = 1; mainLoop < UNIT_MAX; mainLoop++)
            {
                innerLoop = mainLoop;
                while (innerLoop >= 1)
                {
                    String one = "";
                    if (Nodes[sortedIndex[innerLoop]].nodeName != null)
                        one = Nodes[sortedIndex[innerLoop]].nodeName;
                    String two = "";
                    if (Nodes[sortedIndex[innerLoop - 1]].nodeName != null)
                        two = Nodes[sortedIndex[innerLoop - 1]].nodeName;

                    if (arrayLessThan(one, two))
                    {
                        switchArray(innerLoop);
                    }
                    innerLoop--;
                }
            }
        }

        //********************************************************************************
        // Device Sort routine, actual sorting
        //********************************************************************************
        static void sortDeviceArrayGroup()
        {
            int innerLoop;
            int mainLoop;
            for (mainLoop = 1; mainLoop < UNIT_MAX; mainLoop++)
            {
                innerLoop = mainLoop;
                while (innerLoop >= 1)
                {
                    String one = "";
                    if (Nodes[sortedIndex[innerLoop]].group != null)
                        one = Nodes[sortedIndex[innerLoop]].group;
                    String two = "";
                    if (Nodes[sortedIndex[innerLoop - 1]].group != null)
                        two = Nodes[sortedIndex[innerLoop - 1]].group;

                    if (arrayLessThan(one, two))
                    {
                        switchArray(innerLoop);
                    }
                    innerLoop--;
                }
            }
        }

    }
}
