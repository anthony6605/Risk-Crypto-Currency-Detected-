output "instance_id" {
  value = aws_instance.server.id
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "shell" {
  description = "Open a shell on the server."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.server.id}"
}

output "port_forwards" {
  description = "Run one per terminal, then open http://localhost:<port>."
  value = {
    for name, port in { dashboard = 8501, dagster = 3000, grafana = 3001, prometheus = 9090 } :
    name => "aws ssm start-session --region ${var.region} --target ${aws_instance.server.id} --document-name AWS-StartPortForwardingSession --parameters portNumber=${port},localPortNumber=${port}"
  }
}
