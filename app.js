
var fs = require('fs');
var path = "/home/jayam/Downloads/kafka_2.11-0.10.0.0/fiscal.txt";

function writeToFile(data) {
    fs.appendFile(path, "\n"+data, function(err) {
        if(err) {
            return console.log(err);
        }
    });

}
//
// for(i=0;i<50;i++){
//     // writeToFile(' "some", "data", "from", "ZZZZ", "ZZZZ", "ZZZZ", "ZZZZ", "ZZZZ", "ZZZZ", "ZZZZ", "ZZZZ"   ');
//     writeToFile('"KORZZZ","KoreaZZZ","D62_D631XXCGZZZ","Social benefits & transfers in kind - purchased market production, payableZZZ","050ZZZ","Environment protectionZZZ","GS1312ZZZ","State ZZZgovernmentZZZ","CZZZ","Current pricesZZZ","2009ZZZ","2009ZZZ","KRWZZZ","WonZZZ","6ZZZ","MillionsZZZ",,,0,,');
// }

readline = require("readline");
var file = "dataShort.csv";
var cursorY =0;
var rl = readline.createInterface({
    input: fs.createReadStream(file),
    output: null,
    terminal: false
})

rl.on("line", function(line) {
    console.log(cursorY+": " + line);
    writeToFile(line);
    cursorY++;
});

rl.on("close", function() {
    console.log("All data processed, Lines Read "+cursorY);
});

