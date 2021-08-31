setup:
	export AWS_PROFILE=tc-main
	export CDK_DEFAULT_ACCOUNT=tc-main
	export CDK_DEFAULT_REGION=ap-southeast-2
	pip install -r requirements.txt
	echo 'Dev dependencies installed.'
	echo 'Run $cdk synth to create cfn template.'
	echo 'Run $cdk deploy to deploy stacks.'
