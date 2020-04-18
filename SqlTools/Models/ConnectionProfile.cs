using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlTools.Models
{
    public class ConnectionProfile
    {
        public string Server { get; set; }
        public string Database { get; set; }
        public string User { get; set; }
        public string Pass { get; set; }

        public string GeneratedConstring=>  "Server=" + Server + ";Database=" + Database + ";" + (User == string.Empty ? "" : "uid=" + User + ";") +
                   (Pass == string.Empty ? "" : "Password=" + Pass + ";");
        
    }
}
