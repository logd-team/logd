package lib

/*************
* 配置读取函数，配置格式为ini格式
*
*****************/
import (
    "bufio"
    "os"
    "log"
    "strings"
)

func ReadConfig(cfgFile string) map[string]map[string]string {
    fin, err := os.Open(cfgFile)
    if err != nil {
        log.Fatal(err)
    }
    config := make(map[string]map[string]string)
    config[""] = make(map[string]string)
    var section = ""
    scanner := bufio.NewScanner(fin)
    //逐行读取
    for scanner.Scan() {
        line := strings.Trim(scanner.Text(), " ")
        if line == "" || line[0] == ';' {
            //这行是注释，跳过
            continue
        }
        lSqr := strings.Index(line, "[")
        rSqr := strings.Index(line, "]")
        if lSqr == 0 && rSqr == len(line)-1 {
            section = line[lSqr+1 : rSqr]
            _, ok := config[section]
            if !ok {
                config[section] = make(map[string]string)
            }
            continue;
        }

        parts := strings.Split(line, "=")
        if len(parts) == 2 {
            key := strings.Trim(parts[0], " ")
            val := strings.Trim(parts[1], " ")
            config[section][key] = val
        }
    }
    fin.Close()
    return config
}
