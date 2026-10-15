const express = require("express");
const http = require("http");
const path = require("path");

const {
    WebSocketServer,
    WebSocket
} = require("ws");


const app = express();


// publicフォルダーをWebページとして公開
app.use(
    express.static(
        path.join(__dirname, "public")
    )
);


// 動作確認用
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


// roomごとの接続を保存
//
// rooms = {
//     demo: {
//         phone: Set,
//         unity: Set
//     }
// }
const rooms = new Map();


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
        room.phone.size === 0 &&
        room.unity.size === 0
    ) {
        rooms.delete(roomName);
    }
}


// WebSocket接続
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
            role !== "phone" &&
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


        // heartbeat用
        ws.isAlive = true;


        ws.on(
            "pong",
            () => {

                ws.isAlive = true;

            }
        );


        ws.on(
            "message",
            (data, isBinary) => {

                // 今回はスマホ → Unityだけ転送
                if (role !== "phone") {
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
                                binary: isBinary
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

                cleanupRoom(roomName);

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


// 死んでいる接続を掃除
const heartbeatInterval =
    setInterval(
        () => {

            for (
                const ws
                of wss.clients
            ) {

                if (ws.isAlive === false) {

                    ws.terminate();

                    continue;
                }


                ws.isAlive = false;

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
    process.env.PORT || 10000;


server.listen(
    port,
    "0.0.0.0",
    () => {

        console.log(
            `Gyro Maze Relay started on port ${port}`
        );

    }
);
