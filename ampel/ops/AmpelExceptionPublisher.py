#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ops/AmpelExceptionPublisher.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 03.09.2018
# Last Modified Date: 27.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>


import datetime, json, socket
from bson import ObjectId
from slack import WebClient
from slack.web.slack_response import SlackResponse
from typing import Any, Dict, List, Optional
from ampel.abstract.AbsOpsUnit import AbsOpsUnit
from ampel.secret.NamedSecret import NamedSecret


class AmpelExceptionPublisher(AbsOpsUnit):

    slack_token: NamedSecret[str] = NamedSecret(label="slack/operator")
    user: str = f"ampel@{socket.gethostname()}"
    channel: str = "ampel-troubles"
    dry_run: bool = False
    quiet: bool = True

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.slack = WebClient(self.slack_token.get())
        self.troubles = self.context.db.get_collection("troubles", "r")

    def t3_fields(self, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        fields = []
        if "job" in doc:
            fields.append(
                {"title": "Job", "value": doc.get("job", None), "short": True}
            )
        if "task" in doc:
            fields.append(
                {"title": "Task", "value": doc.get("task", None), "short": True}
            )
        fields.append({"title": "Run", "value": doc.get("run", None), "short": True})
        return fields

    def format_attachment(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        fields = [
            {"title": "tier", "value": doc["tier"], "short": True},
        ]
        if "process" in doc:
            fields.append({"title": "process", "value": doc["process"], "short": True})
        if "run" in doc:
            fields.append({"title": "run", "value": doc["run"], "short": True})
        if doc["tier"] == 0:
            for field in "section", "stock":
                fields.append(
                    {"title": field, "value": doc.get(field, None), "short": True}
                )
            if "alert" in doc:
                fields.append(
                    {"title": "alert", "value": doc["alert"], "short": True}
                )
        elif doc["tier"] == 2:
            fields.append(
                {"title": "unit", "value": doc.get("unit", None), "short": True}
            )
            t2Doc = doc.get("t2Doc", None)
            if hasattr(t2Doc, "binary"):
                fields.append(
                    {"title": "t2Doc", "value": t2Doc.binary.hex(), "short": True}
                )
        if "exception" in doc:
            text = "```{}```".format("\n".join(doc["exception"]))
        elif "location" in doc:
            text = "{}: {}".format(doc["location"], doc.get("ampelMsg", ""))
            if "mongoUpdateResult" in doc:
                text += "\nmongoUpdateResult: `{}`".format(doc["mongoUpdateResult"])
            elif "errDict" in doc:
                text += "```\n{}```".format(repr(doc["errDict"]))
        else:
            text = "Unknown exception type. Doc keys are: ```{}```".format(doc.keys())

        attachment = {
            "fields": fields,
            "ts": int(doc["_id"].generation_time.timestamp()),
            "text": text,
            "mrkdwn_in": ["text"],
        }
        return attachment

    def run(self, beacon: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:

        now = datetime.datetime.utcnow()
        t0 = beacon["updated"] if beacon else now - datetime.timedelta(hours=1)
        dt = now - t0

        attachments: List[Dict[str, Any]] = []
        message = {
            "attachments": attachments,
            "channel": "#" + self.channel,
            "username": self.user,
            "as_user": False,
        }

        cursor = self.troubles.find({"_id": {"$gt": ObjectId.from_datetime(t0)}})
        for doc in cursor:
            if len(attachments) < 20:
                attachments.append(self.format_attachment(doc))
            else:
                break

        if dt.days > 3:
            time_range = "{} days".format(dt.days)
        elif dt.days > 0 or dt.seconds > 2 * 3600:
            time_range = "{} hours".format(int(dt.days * 24 + dt.seconds / 3600))
        elif dt.seconds > 2 * 60:
            time_range = "{} minutes".format(int(dt.seconds / 60))
        else:
            time_range = "{} seconds".format(int(dt.seconds))

        count = cursor.count()
        if len(attachments) < count:
            message[
                "text"
            ] = f"Here are the first {len(attachments)} exceptions. There were {count-len(attachments)} more in the last {time_range}."
        else:
            message[
                "text"
            ] = f"There were {len(attachments)} exceptions in the last {time_range}."

        if self.dry_run:
            self.logger.info(json.dumps(message, indent=1))
        elif attachments or not self.quiet:
            result = self.slack.api_call("chat.postMessage", json=message)
            if isinstance(result, SlackResponse):
                if not result["ok"]:
                    raise RuntimeError(result["error"])
            else:
                raise TypeError(f"Sync client returned a future {result}")
        self.logger.info(f"{count} exceptions in the last {time_range}".format(count))

        return {"updated": now}
