const express = require("express");
const http = require("http");
const path = require("path");

const {
    WebSocketServer,
    WebSocket
} = require("ws");


const app = express();


app.use(
    express.static(
        path.join(__dirname, "public")
    )
);


app.get("/health", (req, res) => {

    res.json({
        ok: true,
        service: "Gyro Maze Relay"
    });

});


const server =
    http.createServer(app);


const wss =
    new WebSocketServer({
        server,
        path: "/ws"
    });


const rooms =
    new Map();


function getRoom(roomName) {

    if (!rooms.has(roomName)) {

        rooms.set(
            roomName,
            {
                phone: new Set(),
                unity: new Set()
            }
        );
    }

    return rooms.get(roomName);
}


function cleanupRoom(roomName) {

    const room =
        rooms.get(roomName);


    if (!room) {
        return;
    }


    if (
        room.phone.size === 0
        &&
        room.unity.size === 0
    ) {

        rooms.delete(roomName);
    }
}


function sendJson(
    ws,
    data
) {

    if (
        ws.readyState
        !== WebSocket.OPEN
    ) {
        return;
    }


    ws.send(
        JSON.stringify(data)
    );
}


function broadcastJson(
    clients,
    data
) {

    const json =
        JSON.stringify(data);


    for (
        const client
        of clients
    ) {

        if (
            client.readyState
            === WebSocket.OPEN
        ) {

            client.send(json);
        }
    }
}


wss.on(
    "connection",
    (ws, request) => {

        const url =
            new URL(
                request.url,
                "http://localhost"
            );


        const roomName =
            (
                url.searchParams.get("room")
                || "demo"
            )
            .slice(0, 64);


        const role =
            url.searchParams.get("role");


        if (
            role !== "phone"
            &&
            role !== "unity"
        ) {

            ws.close(
                1008,
                "Invalid role"
            );

            return;
        }


        const room =
            getRoom(roomName);


        room[role].add(ws);


        console.log(
            `[connect] room=${roomName} role=${role}`
        );


        ws.isAlive =
            true;


        ws.on(
            "pong",
            () => {

                ws.isAlive =
                    true;
            }
        );


        // =====================================
        // Unityが接続した
        // =====================================

        if (
            role === "unity"
        ) {

            // スマホへ
            // 「Unityが操作受付可能になった」
            // と通知する

            broadcastJson(
                room.phone,
                {
                    type:
                        "unity-ready"
                }
            );
        }


        // =====================================
        // スマホが後から接続した
        // =====================================

        if (
            role === "phone"
            &&
            room.unity.size > 0
        ) {

            // Unityがすでにいるなら
            // このスマホへ即通知

            sendJson(
                ws,
                {
                    type:
                        "unity-ready"
                }
            );
        }


        ws.on(
            "message",
            (data, isBinary) => {

                // 今回ゲーム操作は
                // phone → unityだけ

                if (
                    role !== "phone"
                ) {
                    return;
                }


                for (
                    const unityClient
                    of room.unity
                ) {

                    if (
                        unityClient.readyState
                        === WebSocket.OPEN
                    ) {

                        unityClient.send(
                            data,
                            {
                                binary:
                                    isBinary
                            }
                        );
                    }
                }
            }
        );


        ws.on(
            "close",
            () => {

                room[role].delete(ws);


                // 最後のUnityが消えたら
                // スマホへ通知

                if (
                    role === "unity"
                    &&
                    room.unity.size === 0
                ) {

                    broadcastJson(
                        room.phone,
                        {
                            type:
                                "unity-disconnected"
                        }
                    );
                }


                cleanupRoom(
                    roomName
                );


                console.log(
                    `[disconnect] room=${roomName} role=${role}`
                );
            }
        );


        ws.on(
            "error",
            (error) => {

                console.error(error);
            }
        );
    }
);


const heartbeatInterval =
    setInterval(
        () => {

            for (
                const ws
                of wss.clients
            ) {

                if (
                    ws.isAlive
                    === false
                ) {

                    ws.terminate();

                    continue;
                }


                ws.isAlive =
                    false;


                ws.ping();
            }

        },
        30000
    );


wss.on(
    "close",
    () => {

        clearInterval(
            heartbeatInterval
        );
    }
);


const port =
    process.env.PORT
    || 10000;


server.listen(
    port,
    "0.0.0.0",
    () => {

        console.log(
            `Gyro Maze Relay started on port ${port}`
        );
    }
);
