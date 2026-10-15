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


function broadcastPeerStatus(
    roomName
) {

    const room =
        rooms.get(roomName);


    if (!room) {
        return;
    }


    const status = {

        type:
            "peer_status",

        unityConnected:
            room.unity.size > 0,

        phoneConnected:
            room.phone.size > 0
    };


    for (
        const client
        of room.phone
    ) {

        sendJson(
            client,
            status
        );
    }


    for (
        const client
        of room.unity
    ) {

        sendJson(
            client,
            status
        );
    }
}


function cleanupRoom(
    roomName
) {

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

        rooms.delete(
            roomName
        );
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
            .slice(
                0,
                64
            );


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
            getRoom(
                roomName
            );


        room[role].add(
            ws
        );


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


        // 接続状況を両者へ通知
        broadcastPeerStatus(
            roomName
        );


        ws.on(
            "message",
            (
                data,
                isBinary
            ) => {

                // -------------------------
                // スマホ → Unity
                // Unity → スマホ
                // の両方を許可
                // -------------------------

                const targets =
                    role === "phone"
                    ? room.unity
                    : room.phone;


                for (
                    const client
                    of targets
                ) {

                    if (
                        client.readyState
                        === WebSocket.OPEN
                    ) {

                        client.send(
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

                room[role].delete(
                    ws
                );


                console.log(
                    `[disconnect] room=${roomName} role=${role}`
                );


                broadcastPeerStatus(
                    roomName
                );


                cleanupRoom(
                    roomName
                );
            }
        );


        ws.on(
            "error",
            (error) => {

                console.error(
                    error
                );
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
                    ws.isAlive === false
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
