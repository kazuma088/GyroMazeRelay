const express = require("express");
const http = require("http");
const path = require("path");

const {
    WebSocketServer,
    WebSocket
} = require("ws");


const app = express();


const RELAY_INTERVAL_MS =
    20;


// publicフォルダーをWebページとして公開
app.use(
    express.static(
        path.join(__dirname, "public")
    )
);


// 動作確認用
app.get(
    "/health",
    (req, res) => {

        res.json({
            ok: true,
            service: "Gyro Maze Relay"
        });
    }
);


const server =
    http.createServer(app);


const wss =
    new WebSocketServer({
        server,
        path: "/ws"
    });


// ==================================================
// Room
// ==================================================

const rooms =
    new Map();


function getRoom(
    roomName
) {

    if (
        !rooms.has(roomName)
    ) {

        rooms.set(
            roomName,
            {
                phone:
                    new Set(),

                unity:
                    new Set(),

                readyUnity:
                    new Set(),

                latestOrientation:
                    null,

                orientationVersion:
                    0,

                unityGeneration:
                    0
            }
        );
    }


    return rooms.get(
        roomName
    );
}


function cleanupRoom(
    roomName
) {

    const room =
        rooms.get(
            roomName
        );


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


function sendJson(
    ws,
    value
) {

    if (
        ws.readyState
        !== WebSocket.OPEN
    ) {

        return;
    }


    ws.send(
        JSON.stringify(
            value
        )
    );
}


function notifyPhones(
    room
) {

    const unityReady =
        room.readyUnity.size > 0;


    for (
        const phoneClient
        of room.phone
    ) {

        sendJson(
            phoneClient,
            {
                type:
                    "relay_status",

                unityReady:
                    unityReady,

                unityGeneration:
                    room.unityGeneration
            }
        );
    }
}


function clearLatestOrientation(
    room
) {

    room.latestOrientation =
        null;


    room.orientationVersion =
        0;


    for (
        const unityClient
        of room.unity
    ) {

        unityClient.lastSentVersion =
            -1;
    }
}


// ==================================================
// WebSocket connection
// ==================================================

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
            url.searchParams.get(
                "role"
            );


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


        ws.role =
            role;


        ws.roomName =
            roomName;


        ws.isRelayReady =
            false;


        ws.lastSentVersion =
            -1;


        ws.isAlive =
            true;


        room[role].add(
            ws
        );


        console.log(
            `[connect] room=${roomName} role=${role}`
        );


        if (
            role === "phone"
        ) {

            sendJson(
                ws,
                {
                    type:
                        "relay_status",

                    unityReady:
                        room.readyUnity.size > 0,

                    unityGeneration:
                        room.unityGeneration
                }
            );
        }


        // ==============================================
        // heartbeat
        // ==============================================

        ws.on(
            "pong",
            () => {

                ws.isAlive =
                    true;
            }
        );


        // ==============================================
        // message
        // ==============================================

        ws.on(
            "message",
            (data, isBinary) => {

                if (isBinary) {

                    return;
                }


                const text =
                    data.toString(
                        "utf8"
                    );


                if (
                    text.length > 8192
                ) {

                    return;
                }


                let message;


                try {

                    message =
                        JSON.parse(
                            text
                        );
                }
                catch {

                    return;
                }


                // ======================================
                // Unity -> Server
                // ======================================

                if (
                    role === "unity"
                ) {

                    if (
                        message.type
                        !== "unity_ready"
                    ) {

                        return;
                    }


                    if (
                        ws.isRelayReady
                    ) {

                        return;
                    }


                    const hadReadyUnity =
                        room.readyUnity.size > 0;


                    ws.isRelayReady =
                        true;


                    ws.lastSentVersion =
                        -1;


                    room.readyUnity.add(
                        ws
                    );


                    room.unityGeneration +=
                        1;


                    if (
                        !hadReadyUnity
                    ) {

                        clearLatestOrientation(
                            room
                        );
                    }


                    console.log(
                        `[ready] room=${roomName} role=unity`
                    );


                    notifyPhones(
                        room
                    );


                    return;
                }


                // ======================================
                // Phone -> Server
                // ======================================

                if (
                    message.type
                    !== "orientation"
                ) {

                    return;
                }


                if (
                    room.readyUnity.size === 0
                ) {

                    return;
                }


                if (
                    typeof message.sessionId
                        !== "string"
                    ||
                    typeof message.calibrationId
                        !== "number"
                    ||
                    typeof message.sequence
                        !== "number"
                    ||
                    typeof message.x
                        !== "number"
                    ||
                    typeof message.y
                        !== "number"
                    ||
                    typeof message.z
                        !== "number"
                    ||
                    typeof message.w
                        !== "number"
                ) {

                    return;
                }


                room.latestOrientation =
                    message;


                room.orientationVersion +=
                    1;
            }
        );


        // ==============================================
        // close
        // ==============================================

        ws.on(
            "close",
            () => {

                room[role].delete(
                    ws
                );


                if (
                    role === "unity"
                ) {

                    room.readyUnity.delete(
                        ws
                    );


                    if (
                        room.readyUnity.size === 0
                    ) {

                        clearLatestOrientation(
                            room
                        );
                    }


                    notifyPhones(
                        room
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

                console.error(
                    error
                );
            }
        );
    }
);


// ==================================================
// Latest-state relay
// ==================================================

const relayInterval =
    setInterval(
        () => {

            const relaySentAtMs =
                Date.now();


            for (
                const room
                of rooms.values()
            ) {

                if (
                    room.readyUnity.size === 0
                    ||
                    room.latestOrientation === null
                ) {

                    continue;
                }


                const version =
                    room.orientationVersion;


                const payload =
                    JSON.stringify({

                        ...room.latestOrientation,

                        relaySentAtMs:
                            relaySentAtMs
                    });


                for (
                    const unityClient
                    of room.readyUnity
                ) {

                    if (
                        unityClient.readyState
                            !== WebSocket.OPEN
                        ||
                        unityClient.lastSentVersion
                            === version
                    ) {

                        continue;
                    }


                    if (
                        unityClient.bufferedAmount
                        !== 0
                    ) {

                        continue;
                    }


                    unityClient.send(
                        payload
                    );


                    unityClient.lastSentVersion =
                        version;
                }
            }

        },

        RELAY_INTERVAL_MS
    );


// ==================================================
// heartbeat
// ==================================================

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
            relayInterval
        );


        clearInterval(
            heartbeatInterval
        );
    }
);


// ==================================================
// Start
// ==================================================

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
