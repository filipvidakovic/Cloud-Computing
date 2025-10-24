# projekat/frontend/s3_cloudfront_stack.py
from aws_cdk import (
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    RemovalPolicy,
    CfnOutput,
    Duration
)
from constructs import Construct
from ..config import PROJECT_PREFIX
import os

class FrontendStack(Construct):
    def __init__(self, scope: Construct, id: str, api_gateway_url: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        # S3 Bucket for hosting
        
        bucket = s3.Bucket(
            self, "FrontendBucket",
            website_index_document="index.html",
            website_error_document="index.html",
            public_read_access=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ACLS,  # 👈 OVO JE KLJUČNO
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        
        # CloudFront Distribution
        distribution = cloudfront.Distribution(
            self, "FrontendDistribution",
            default_root_object="index.html",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
            ),
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html"
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html"
                )
            ],
            price_class=cloudfront.PriceClass.PRICE_CLASS_100  # Cheapest
        )
        
        # Find the frontend build path
        # Prilagodite putanju prema vašoj strukturi
        current_dir = os.getcwd()
        frontend_path = os.path.join(current_dir, "..", "frontend-cloud")
        dist_path = os.path.join(frontend_path, "dist")
        
        print(f"🔍 Looking for frontend build at: {dist_path}")
        
        # Deploy to S3
        deployment = s3deploy.BucketDeployment(
            self, "FrontendDeployment",
            sources=[s3deploy.Source.asset(dist_path)],
            destination_bucket=bucket,
            distribution=distribution,
            distribution_paths=["/*"],
            memory_limit=512
        )
        
        # Outputs
        CfnOutput(self, "CloudFrontURL", 
                  value=f"https://{distribution.distribution_domain_name}")
        CfnOutput(self, "S3WebsiteURL", value=bucket.bucket_website_url)