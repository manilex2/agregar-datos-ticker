require('dotenv').config();
const express = require('express');
const morgan = require('morgan');
const app = express();
const mysql = require('mysql2');
const { database } = require('./keys');
const {google} = require('googleapis');
const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: "https://www.googleapis.com/auth/spreadsheets"
});
const PUERTO = 4600;

app.use(morgan('dev'));

app.get('/', async function (solicitud, respuesta) {
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

    async function agregarDatos(recogerDatos) {
        try {
            if(recogerDatos) {
                for(i = 0; i < recogerDatos.length; i++){
                    var name = recogerDatos[i][0].toString();
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
                    arreglo.push([name, indice, fecha, preop, propost, p1, p2, p3, p4, pm, pm2, sl, sl2]);
                };
                for (let i = 0; i < arreglo.length; i++) {
                    let sql = `INSERT INTO datos (${process.env.CRIPTOTICKER}, indice, fecha, preop, propost, p1, p2, p3, p4, pm, pm2, sl, sl2)
                    SELECT * FROM (SELECT
                        '${arreglo[i][0]}' AS ${process.env.CRIPTOTICKER},
                        '${arreglo[i][1]}' AS indice,
                        '${arreglo[i][2]}' AS fecha,
                        ${arreglo[i][3]} AS preop,
                        ${arreglo[i][4]} AS propost,
                        ${arreglo[i][5]} AS p1,
                        ${arreglo[i][6]} AS p2,
                        ${arreglo[i][7]} AS p3,
                        ${arreglo[i][8]} AS p4,
                        ${arreglo[i][9]} AS pm,
                        ${arreglo[i][10]} AS pm2,
                        ${arreglo[i][11]} AS sl,
                        ${arreglo[i][12]} AS sl2) AS tmp
                    WHERE NOT EXISTS (
                        SELECT indice, fecha FROM datos WHERE indice = '${arreglo[i][1]}' AND fecha = '${arreglo[i][2]}'
                    ) LIMIT 1`;
                    conexion.query(sql, function (err, resultado) {
                        if (err) throw err;
                        console.log(resultado);
                    });
                }
                await finalizarEjecucion();
            } else {
                console.log("No habían datos en Source cuando se hizo la solicitud.");
                await finalizarEjecucion();
            }
        } catch (error) {
            console.error(error)
        }
    };
    async function finalizarEjecucion() {
        conexion.end();
        respuesta.send("Ejecutado");
    }
});

app.listen(PUERTO || process.env.PORT, () => {
    console.log(`Escuchando en el puerto ${PUERTO || process.env.PORT}`);
});
