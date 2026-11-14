import asyncio
import smtplib
from html import escape
from email.message import EmailMessage
from app.config import settings

def _format_rca(incident, rca):
    def items(key):
        return "".join(f"<li>{escape(str(x))}</li>" for x in rca.get(key, []))
    whys = "".join(
        f"<li><b>{escape(str(w.get('why','Why?')))}</b> {escape(str(w.get('answer','')))}</li>"
        for w in rca.get("five_whys", [])
    )
    return f"""<h2>Kubernetes incident RCA</h2>
<p><b>Incident:</b> {escape(incident.id)} | <b>Severity:</b> {escape(incident.severity)}</p>
<p><b>Cluster:</b> {escape(settings.cluster_name)} | <b>Namespace:</b> {escape(incident.namespace)}
| <b>Pod:</b> {escape(incident.pod_name)} | <b>Node:</b> {escape(incident.node_name)}</p>
<h3>Summary</h3><p>{escape(str(rca.get('incident_summary','')))}</p>
<h3>Root cause ({escape(str(rca.get('confidence','unknown')))} confidence)</h3>
<p>{escape(str(rca.get('root_cause','')))}</p>
<h3>Evidence</h3><ul>{items('evidence')}</ul>
<h3>5 Whys</h3><ol>{whys}</ol>
<h3>Impact</h3><p>{escape(str(rca.get('impact','Unknown')))}</p>
<h3>Immediate corrective actions</h3><ul>{items('immediate_actions')}</ul>
<h3>Preventive actions</h3><ul>{items('preventive_actions')}</ul>
<h3>Suggested kubectl commands</h3><ul>{items('kubectl_commands')}</ul>
<h3>Further investigation</h3><ul>{items('further_investigation')}</ul>
<p><i>AI-generated analysis; validate recommendations before execution. No remediation was performed.</i></p>"""

async def send_incident_email(incident, rca):
    if not settings.recipients:
        return "SKIPPED_NO_RECIPIENTS"
    if not settings.smtp_host or not settings.smtp_from:
        return "SKIPPED_SMTP_NOT_CONFIGURED"
    subject = f"[{incident.severity}][{settings.cluster_name}] {incident.namespace}/{incident.pod_name}"
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.smtp_from
    message["To"] = ", ".join(settings.recipients)
    if settings.email_cc:
        message["Cc"] = settings.email_cc
    message.set_content("Kubernetes incident RCA report. Please view this message in an HTML-capable email client.")
    message.add_alternative(_format_rca(incident, rca), subtype="html")
    recipients = list(settings.recipients) + [x.strip() for x in settings.email_cc.split(",") if x.strip()]

    def deliver():
        if settings.smtp_use_ssl:
            server = smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, timeout=20)
        else:
            server = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20)
        try:
            server.ehlo()
            if settings.smtp_starttls and not settings.smtp_use_ssl:
                server.starttls()
                server.ehlo()
            if settings.smtp_username:
                server.login(settings.smtp_username, settings.smtp_password)
            server.send_message(message, to_addrs=recipients)
        finally:
            server.quit()

    await asyncio.to_thread(deliver)
    return "SENT"
