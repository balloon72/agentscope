import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';

import type { AgentView, TeamPipelineConfig } from '@/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import {
	Field,
	FieldContent,
	FieldDescription,
	FieldLabel,
	FieldLegend,
	FieldSet,
} from '@/components/ui/field';
import { Input } from '@/components/ui/input';
import {
	duplicateTeamPipelineNames,
	teamPipelineCandidates,
	toggleTeamPipelineMember,
} from '@/lib/team-pipeline';

interface Props {
	value: TeamPipelineConfig;
	onChange: (value: TeamPipelineConfig) => void;
	agents: AgentView[];
	currentAgentId?: string;
	loading?: boolean;
	error?: Error | null;
	onRetry?: () => void;
}

/** Agent references need a picker; the generic SchemaForm only handles scalars. */
export function TeamPipelineFormSection({
	value,
	onChange,
	agents,
	currentAgentId,
	loading,
	error,
	onRetry,
}: Props) {
	const { t } = useTranslation();
	const id = useId();
	const [query, setQuery] = useState('');
	const candidates = teamPipelineCandidates(agents, currentAgentId);
	const selectedIds = new Set(value.member_agent_ids);
	const availableIds = new Set(candidates.map((agent) => agent.id));
	const missingIds = value.member_agent_ids.filter((agentId) => !availableIds.has(agentId));
	const duplicateNames = duplicateTeamPipelineNames(candidates);
	const matches = candidates.filter((agent) =>
		`${agent.data.name} ${agent.id}`
			.toLocaleLowerCase()
			.includes(query.trim().toLocaleLowerCase()),
	);
	const toggleMember = (agentId: string, selected: boolean) =>
		onChange({
			...value,
			member_agent_ids: toggleTeamPipelineMember(value.member_agent_ids, agentId, selected),
		});

	return (
		<FieldSet>
			<FieldLegend>{t('agent-form.team-pipeline-config.legend')}</FieldLegend>
			<FieldDescription>{t('agent-form.team-pipeline-config.description')}</FieldDescription>
			<Field orientation="horizontal">
				<Checkbox
					id={`${id}-enabled`}
					checked={value.enabled}
					onCheckedChange={(checked) => onChange({ ...value, enabled: checked === true })}
				/>
				<FieldContent>
					<FieldLabel htmlFor={`${id}-enabled`}>
						{t('agent-form.team-pipeline-config.enabled')}
					</FieldLabel>
					<FieldDescription>
						{t('agent-form.team-pipeline-config.enabled-description')}
					</FieldDescription>
				</FieldContent>
			</Field>
			{value.enabled && (
				<Field>
					<FieldLabel htmlFor={`${id}-search`}>
						{t('agent-form.team-pipeline-config.members', {
							count: value.member_agent_ids.length,
						})}
					</FieldLabel>
					<Input
						id={`${id}-search`}
						value={query}
						onChange={(event) => setQuery(event.target.value)}
						placeholder={t('agent-form.team-pipeline-config.search')}
					/>
					{loading ? (
						<p role="status" className="text-muted-foreground text-sm">
							{t('common.loading')}
						</p>
					) : error ? (
						<div role="alert" className="text-destructive text-sm">
							<p>{t('agent-form.team-pipeline-config.load-error')}</p>
							{onRetry && (
								<Button type="button" variant="outline" size="sm" onClick={onRetry}>
									{t('agent-form.team-pipeline-config.retry')}
								</Button>
							)}
						</div>
					) : (
						<>
							{duplicateNames.length > 0 && (
								<p role="status" className="text-muted-foreground text-sm">
									{t('agent-form.team-pipeline-config.duplicate-names')}
								</p>
							)}
							<div
								className="max-h-52 space-y-3 overflow-y-auto rounded-md border p-3"
								aria-label={t('agent-form.team-pipeline-config.legend')}
							>
								{matches.map((agent) => (
									<Field
										key={agent.id}
										orientation="horizontal"
										className="gap-3"
									>
										<Checkbox
											id={`${id}-${agent.id}`}
											checked={selectedIds.has(agent.id)}
											onCheckedChange={(checked) =>
												toggleMember(agent.id, checked === true)
											}
										/>
										<FieldLabel
											htmlFor={`${id}-${agent.id}`}
											className="min-w-0 flex-1 font-normal"
										>
											<span className="min-w-0">
												<span className="block truncate">
													{agent.data.name}
												</span>
												<span className="text-muted-foreground block truncate text-xs">
													{agent.id}
												</span>
											</span>
										</FieldLabel>
										{!agent.editable && (
											<Badge variant="secondary">
												{t('common.readOnly')}
											</Badge>
										)}
									</Field>
								))}
								{matches.length === 0 && (
									<p className="text-muted-foreground text-sm">
										{t(
											candidates.length === 0
												? 'agent-form.team-pipeline-config.empty'
												: 'agent-form.team-pipeline-config.no-matches',
										)}
									</p>
								)}
							</div>
							{missingIds.length > 0 && (
								<div role="alert" className="space-y-2 text-sm">
									<p className="text-destructive">
										{t(
											'agent-form.team-pipeline-config.unavailable-description',
										)}
									</p>
									{missingIds.map((agentId) => (
										<div
											key={agentId}
											className="flex items-center justify-between gap-2"
										>
											<span className="truncate">{agentId}</span>
											<Button
												type="button"
												variant="outline"
												size="sm"
												onClick={() => toggleMember(agentId, false)}
												aria-label={t(
													'agent-form.team-pipeline-config.remove-agent',
													{ id: agentId },
												)}
											>
												{t('agent-form.team-pipeline-config.remove')}
											</Button>
										</div>
									))}
								</div>
							)}
						</>
					)}
					{value.member_agent_ids.length === 0 && (
						<FieldDescription>
							{t('agent-form.team-pipeline-config.required')}
						</FieldDescription>
					)}
				</Field>
			)}
		</FieldSet>
	);
}
