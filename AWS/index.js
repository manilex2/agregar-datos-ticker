require('dotenv').config();
const mysql = require('mysql2');
const { database } = require('./keys');
const {google} = require('googleapis');
const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: "https://www.googleapis.com/auth/spreadsheets"
});

exports.handler = async function (event) {
    const promise = new Promise(async function() {
        const conexion = mysql.createConnection({
            host: database.host,
            user: database.user,
            password: database.password,
            port: database.port,
            database: database.database
        });
        const spreadsheetId = process.env.SPREADSHEET_ID;
        const client = await auth.getClient();
        const googleSheet = google.sheets({ version: 'v4', auth: client });
        var arreglo = [];
        var request = (await googleSheet.spreadsheets.values.get({
                    auth,
                    spreadsheetId,
                    range: `${process.env.ID_HOJA}!A2:L`
                })).data;
        var recogerDatos = request.values;
        agregarDatos(recogerDatos);
    
        function agregarDatos(recogerDatos) {
            for(i = 0; i < recogerDatos.length; i++){
                var ticker = recogerDatos[i][0].toString();
                var indice = recogerDatos[i][1].toString();
                var fecha = recogerDatos[i][2].toString();
                if (!recogerDatos[i][3]) {
                    var preop = 0;
                } else {
                    var preop = parseFloat(recogerDatos[i][3]);
                }
                if (!recogerDatos[i][4]) {
                    var propost = 0;
                } else {
                    var propost = parseFloat(recogerDatos[i][4]);
                }
                if (!recogerDatos[i][5]) {
                    var p1 = 0;
                } else {
                    var p1 = parseFloat(recogerDatos[i][5]);
                }
                if (!recogerDatos[i][6]) {
                    var p2 = 0;
                } else {
                    var p2 = parseFloat(recogerDatos[i][6]);
                }
                if (!recogerDatos[i][7]) {
                    var p3 = 0;
                } else {
                    var p3 = parseFloat(recogerDatos[i][7]);
                }
                if (!recogerDatos[i][8]) {
                    var p4 = 0;
                } else {
                    var p4 = parseFloat(recogerDatos[i][8]);
                }
                if (!recogerDatos[i][9]) {
                    var pm = 0;
                } else {
                    var pm = parseFloat(recogerDatos[i][9]);
                }
                if (!recogerDatos[i][10]) {
                    var pm2 = 0;
                } else {
                    var pm2 = parseFloat(recogerDatos[i][10]);
                }
                if (!recogerDatos[i][11]) {
                    var sl = 0;
                } else {
                    var sl = parseFloat(recogerDatos[i][11]);
                }
                if (!recogerDatos[i][12]) {
                    var sl2 = 0;
                } else {
                    var sl2 = parseFloat(recogerDatos[i][12]);
                }
                arreglo.push([ticker, indice, fecha, preop, propost, p1, p2, p3, p4, pm, pm2, sl, sl2]);
            };
            let sql = "INSERT INTO datos (ticker, indice, fecha, preop, propost, p1, p2, p3, p4, pm, pm2, sl, sl2) VALUES ?";
            conexion.query(sql, [arreglo], function (err, resultado) {
                if (err) throw err;
                console.log(resultado);
                conexion.end();
            });
        };
    });
    return promise;
}