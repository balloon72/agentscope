Warning: truncated output (original token count: 39209)
Total output lines: 3927

# -*- coding: utf-8 -*-
"""The unified agent class in AgentScope library."""
import asyncio
import collections
import json
import inspect
import re
import warnings

from asyncio import Queue
from copy import deepcopy
from fnmatch import fnmatch
from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    Sequence,
    List,
    TYPE_CHECKING,
    Type,
)

import jsonschema
from pydantic import BaseModel

from ._config import ContextConfig, ReActConfig, ModelConfig, InjectionConfig
from ..state import AgentState
from ..state._state import ReplyContext
from ._utils import _ToolCallBatch, Acting, Exit, Reasoning, _resolve_timezone
from .._logging import logger
from .._utils._common import (
    _generate_id,
    _json_loads_with_repair,
    _execute_async_or_sync_func,
)
from ..event import (
    AgentEvent,
    ModelCallEndEvent,
    ModelCallStartEvent,
    ReplyEndEvent,
    ReplyStartEvent,
    TextBlockDeltaEvent,
    TextBlockEndEvent,
    TextBlockStartEvent,
    ThinkingBlockDeltaEvent,
    ThinkingBlockEndEvent,
    ThinkingBlockStartEvent,
    ToolCallDeltaEvent,
    ToolCallEndEvent,
    ToolCallStartEvent,
    ToolResultDataDeltaEvent,
    ToolResultEndEvent,
    ToolResultStartEvent,
    ToolResultTextDeltaEvent,
    RequireUserConfirmEvent,
    RequireExternalExecutionEvent,
    ExternalExecutionResultEvent,
    UserConfirmResultEvent,
    DataBlockStartEvent,
    DataBlockDeltaEvent,
    DataBlockEndEvent,
    ExceedMaxItersEvent,
    ReplyFinishedReason,
    UserInterruptEvent,
    HintBlockEvent,
)
from ..exception import AgentOrientedException
from ..model import (
    ChatResponse,
    ChatUsage,
    ChatModelBase,
    FinishedReason,
)
from ..message import (
    Msg,
    AssistantMsg,
    SystemMsg,
    UserMsg,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
    DataBlock,
    Base64Source,
    URLSource,
    ToolCallState,
    ToolResultState,
    Usage,
    HintBlock,
)
from ..tool import (
    Toolkit,
    ToolBase,
    ToolChunk,
    ToolChoice,
    ToolResponse,
    FunctionTool,
)
from ..permission import (
    PermissionBehavior,
    PermissionEngine,
    PermissionDecision,
    PermissionRule,
)
from ._structured_output_tool import _GenerateStructuredOutput
from ..workspace import Offloader, WorkspaceBase

# The name of the tool that the agent uses to compress its own context
_COMPRESSION_TOOL_NAME = "CompressContext"

if TYPE_CHECKING:
    from ..middleware import MiddlewareBase
else:
    MiddlewareBase = Any


class Agent:
    """The agent class."""

    def __init__(
        self,
        name: str,
        system_prompt: str,
        model: ChatModelBase,
        toolkit: Toolkit | None = None,
        middlewares: list[MiddlewareBase] | None = None,
        state: AgentState | None = None,
        offloader: Offloader | None = None,
        # The agent configurations
        model_config: ModelConfig | None = None,
        context_config: ContextConfig | None = None,
        react_config: ReActConfig | None = None,
        injection_config: InjectionConfig | None = None,
    ) -> None:
        """Initialize the agent class in AgentScope.

        Args:
            name (`str`):
                The agent identifier.
            system_prompt (`str`):
                The agent's system prompt. Additional instructions may be
                appended to it dynamically during operation.
            model (`ChatModelBase`):
                The chat model/llm used for this agent.
            toolkit (`Toolkit | None`, optional):
                The toolkit used for registering tools, MCPs and skills as the
                sole source.
            middlewares (`list[MiddlewareBase] | None`, optional):
                Middlewares applied to the agent to modify its behavior
                without altering its source code. Supported hook points
                include: reply, reasoning, permission checking, acting, model
                call, context compression, and system prompt retrieval.
            state (`AgentState | None`, optional):
                The agent state. A new state will be created if not provided.
            offloader (`Offloader | None`, optional):
                The context offloader. If provided, the compressed context and
                tool result will be offloaded.
            model_config (`ModelConfig | None`, optional):
                The additional chat model configuration including fallback
                model and retries.
            context_config (`ContextConfig | None`, optional):
                The context config for context compression and tool result
                compression.
            react_config (`ReActConfig | None`, optional):
                The config for the reasoning-acting loop.
            injection_config (`InjectionConfig | None`, optional):
                The runtime state injection config, which controls how the
                time, (plan) tasks and context usage are injected into the
                context to help the agent better reason and act.
        """
        self.name = name
        self._system_prompt = system_prompt
        self.model = model
        self.state = state or AgentState()

        self.model_config = model_config or ModelConfig()
        self.context_config = context_config or ContextConfig()
        self.react_config = react_config or ReActConfig()
        self.injection_config = injection_config or InjectionConfig()
        if self.injection_config.context_buffer_ratio is not None:
            warnings.warn(
                "The 'context_buffer_ratio' of the injection config is "
                "deprecated, set it in the context config instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.context_config.context_buffer_ratio = (
                self.injection_config.context_buffer_ratio
            )
        self._validate_configs()

        # The permission engine
        self._engine = PermissionEngine(self.state.permission_context)

        # The offloader/workspace
        self.offloader = offloader

        # ====================================================================
        # The Tool-related logics
        # ====================================================================
        self.toolkit = toolkit or Toolkit()
        # Built here rather than in ``_prepare_model_input``, so that the
        # registration can tell this agent's tool from a tool of the same
        # name, e.g. another agent sharing the toolkit
        self._compression_tool = FunctionTool(
            self._compress_context_tool,
            name=_COMPRESSION_TOOL_NAME,
            is_concurrency_safe=False,
            permission=PermissionDecision(
                behavior=PermissionBehavior.ALLOW,
                message=f"{_COMPRESSION_TOOL_NAME} is always allowed.",
            ),
        )

        # ====================================================================
        # The Middleware-related attributes
        # ====================================================================
        # Filter middlewares by implemented hooks (only once)
        middlewares = middlewares or []
        self._reply_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_reply")
        ]
        self._reasoning_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_reasoning")
        ]
        self._check_permission_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_check_permission")
        ]
        self._acting_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_acting")
        ]
        self._model_call_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_model_call")
        ]
        self._system_prompt_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_system_prompt")
        ]
        self._compress_context_middlewares = [
            _ for _ in middlewares if _.is_implemented("on_compress_context")
        ]

        # Set when a `ReplyEndEvent` escapes the reply middleware chain; the
        # reply loop only exits after the event is delivered (not swallowed)
        self._receive_reply_end: bool = False

    def _validate_configs(self) -> None:
        """Validate the config combinations that a single config class cannot
        check by itself.

        Raises:
            `ValueError`:
                If the reserved/buffer ratios don't leave room ahead of the
                context compression threshold.
        """
        if (
            self.context_config.reserve_ratio
            >= self.context_config.trigger_ratio
        ):
            raise ValueError(
                "The 'reserve_ratio' of the context config must be smaller "
                "than its 'trigger_ratio', got "
                f"{self.context_config.reserve_ratio} and "
                f"{self.context_config.trigger_ratio}.",
            )

        # The buffer is only consumed by the runtime state injection and
        # the compression tool, so it's checked only when one of them is on
        if (
            self.injection_config.inject_runtime_state
            or self.context_config.compression_tool_enabled
        ) and (
            self.context_config.context_buffer_ratio
            >= self.context_config.trigger_ratio
        ):
            raise ValueError(
                "The 'context_buffer_ratio' of the context config must be "
                "smaller than its 'trigger_ratio', so that the context "
                "length is injected and the compression tool takes effect "
                "before the hard compression, got "
                f"{self.context_config.context_buffer_ratio} and "
                f"{self.context_config.trigger_ratio}.",
            )

    # =======================================================================
    # Agent public methods
    # =======================================================================

    async def reply_stream(
        self,
        inputs: Msg
        | list[Msg]
        | UserConfirmResultEvent
        | UserInterruptEvent
        | ExternalExecutionResultEvent
        | None = None,
        structured_schema: Type[BaseModel] | None = None,
        yield_final_msg: bool = False,
    ) -> AsyncGenerator[AgentEvent | Msg, None]:
        """Reply to the given inputs and stream agent events.

        Args:
            inputs (`Msg | list[Msg] | UserConfirmResultEvent | \
            UserInterruptEvent | ExternalExecutionResultEvent | None`, \
            optional):
                The inputs that trigger this reply. See :meth:`reply` for
                the full list of accepted variants.
            structured_schema (`Type[BaseModel] | None`, optional):
                The Pydantic model class that the reply's structured output
                must conform to. See :meth:`reply` for details.
            yield_final_msg (`bool`, defaults to `False`):
                If yield the final reply message. When requiring structured
                output, use this option to get the final message, and access
                it via the `structured_output` attribute.

        Yields:
            `AgentEvent | Msg`:
                Streamed events produced during the reply.

        .. note:: If requiring outside interaction for multiple tool calls
            and only receive partial confirmation or execution results, the
            agent won't re-send the requiring events for the unconfirmed
            or unexecuted tool calls.
        """
        async for chunk in self._reply(
            inputs=inputs,
            structured_schema=structured_schema,
        ):
            if isinstance(chunk, Msg) and not yield_final_msg:
                continue
            yield chunk

    async def reply(
        self,
        inputs: Msg
        | list[Msg]
        | UserConfirmResultEvent
        | UserInterruptEvent
        | ExternalExecutionResultEvent
        | None = None,
        structured_schema: Type[BaseModel] | None = None,
    ) -> Msg:
        """Reply to the given inputs, consuming all streamed events.

        Args:
            inputs (`Msg | list[Msg] | UserConfirmResultEvent | \
            UserInterruptEvent | ExternalExecutionResultEvent | None`, \
            optional):
                The inputs that trigger this reply. It can be:

                - a single `Msg` or a list of `Msg` objects to start a new
                  reply,
                - a `UserConfirmResultEvent` or
                  `ExternalExecutionResultEvent` to continue from the
                  outside interaction required by the previous reply,
                - a `UserInterruptEvent` to abort a parked reply — the
                  agent closes all pending tool calls with an interrupted
                  tool result and ends the reply without entering the
                  reasoning-acting loop,
                - `None` if there is nothing new to feed in (e.g. just
                  continue from the current state).
            structured_schema (`Type[BaseModel] | None`, optional):
                The Pydantic model class that the reply's structured output
                must conform to, with the validated result carried on the
                final message's ``structured_output`` attribute as a dict.

        Returns:
            `Msg`:
                A final reply message.
        """
        final_msg: Msg | None = None
        async for evt_or_msg in self._reply(
            inputs=inputs,
            structured_schema=structured_schema,
        ):
            if isinstance(evt_or_msg, Msg):
                final_msg = evt_or_msg
        if final_msg is None:
            raise RuntimeError("Agent did not produce a final message.")
        return final_msg

    async def observe(self, msgs: Msg | list[Msg] | None = None) -> None:
        """Receive external observation message(s) and save them into
        context."""
        await self._handle_incoming_messages(msgs)

    async def compress_context(
        self,
        context_config: ContextConfig | None = None,
        instructions: HintBlock | None = None,
    ) -> None:
        """Compress the agent's context if the token count exceeds the
        threshold.

        Args:
            context_config (`ContextConfig | None`, optional):
                If provided, compress the context with the given context
                config. Otherwise, use the default context config in the
                agent.
            instructions (`HintBlock | None`, optional):
                Optional hints or instructions injected into the compression
                context to guide the summarization behavior.
        """
        if not self._compress_context_middlewares:
            await self._compress_context_impl(
                context_config=context_config,
                instructions=instructions,
            )
        else:

            async def execute_chain(
                index: int = 0,
                context_config: ContextConfig | None = context_config,
                instructions: HintBlock | None = instructions,
            ) -> None:
                """Execute the compress_context middleware chain."""
                if index >= len(self._compress_context_middlewares):
                    await self._compress_context_impl(
                        context_config=context_config,
                        instructions=instructions,
                    )
                else:
                    mw = self._compress_context_middlewares[index]
                    input_kwargs = {
                        "context_config": context_config,
                        "instructions": instructions,
                    }

                    async def next_handler(**kwargs: Any) -> None:
                        await execute_chain(
                            index + 1,
                            **{**input_kwargs, **kwargs},
                        )

                    await mw.on_compress_context(
                        agent=self,
                        input_kwargs=input_kwargs,
                        next_handler=next_handler,
                    )

            await execute_chain()

    async def _compress_context_tool(self) -> ToolChunk:
        """Compress the older context into a continuation summary, while
        preserving the recent context needed for the upcoming work.

        Call it between tasks, where the completed work can be preserved
        accurately in a summary, rather than in the middle of a task. Keep
        the current context when the exact earlier details are still needed.
        The context is replaced only after the summary is generated
        successfully, and is left as-is when it's not long enough to be
        worth compressing.

        Returns:
            `ToolChunk`:
                Whether the context was compressed.
        """
        # The agent decides when to compress, so lower the trigger to the
        # ratio where the compression is recommended. A context below that
        # ratio is left untouched.
        context_config = self.context_config.model_copy(
            update={
                "trigger_ratio": self.context_config.trigger_ratio
                - self.context_config.context_buffer_ratio,
            },
        )

        n_msgs = len(self.state.context)
        try:
            await self.compress_context(context_config=context_config)

        except Exception as e:  # pylint: disable=broad-exception-caught
            return ToolChunk(
                content=[TextBlock(text=f"Context compression failed: {e}")],
                state=ToolResultState.ERROR,
            )

        if len(self.state.context) == n_msgs:
            text = (
                "The context is not long enough to compress, so it remains "
                "unchanged."
            )
        else:
            text = "Context compressed successfully."

        return ToolChunk(
            content=[TextBlock(text=text)],
            state=ToolResultState.SUCCESS,
        )

    async def _compress_context_impl(
        self,
        context_config: ContextConfig | None = None,
        instructions: HintBlock | None = None,
    ) -> None:
        """Compress the agent's context if the token count exceeds the
        threshold.

        Args:
            context_config (`ContextConfig | None`, optional):
                If provided, compress the context with the given context
                config. Otherwise, use the default context config in the
                agent.
            instructions (`HintBlock | None`, optional):
                Optional hints or instructions injected into the compression
                context to guide the summarization behavior.
        """
        cfg: ContextConfig = context_config or self.context_config

        # Limit the number of images in the context first, so that the token
        # counting below reflects the images that actually remain
        await self._limit_context_images(cfg)

        # Count the current tokens
        kwargs = await self._prepare_model_input()
        estimated_tokens = await self.model.count_tokens(**kwargs)

        # Skip if no compression is needed
        threshold = cfg.trigger_ratio * self.model.context_size
        if estimated_tokens < threshold:
            return

        logger.info(
            "[AGENT %s]: Current token count %d exceeds the threshold %d, "
            "activating compression.",
            self.name,
            int(estimated_tokens),
            int(threshold),
        )

        if len(self.state.context) == 0:
            # The system prompt and the summary (if exists) exceeds the
            # threshold, which cannot be compressed, raise the error to the
            # developer!
            suffix = ""
            if self.state.summary:
                suffix = "and the compression summary "
            raise RuntimeError(
                f"The system prompt {suffix}exceed(s) the compression "
                f"threshold ({threshold} tokens), cannot be compressed.",
            )

        # Split the context into the ones to be compressed, and the others to
        # be reserved
        tools = kwargs.get("tools", [])
        (
            msgs_to_compress,
            msgs_to_reserve,
        ) = await self._split_context_for_compression(
            cfg.reserve_ratio * self.model.context_size,
            tools,
        )

        if len(msgs_to_compress) == 0:
            # The reserve ratio is too large so that although it exceeds the
            # trigger threshold, the context to be compressed is empty
            # Fallback by lowering the reserve ratio to compress more context.
            logger.warning(
                "The reserve ratio %.2f is too large to compress any context."
                "Lower the reserve ratio to 0 as a fallback.",
                cfg.reserve_ratio,
            )
            (
                msgs_to_compress,
                msgs_to_reserve,
            ) = await self._split_context_for_compression(
                0 * self.model.context_size,
                tools,
            )

            # The msgs to be compressed cannot be empty here, unless the
            # system prompt and summary (if any) already exceed the context
            # length, which we have handled before.

        # Prepare the messages to compress
        msgs_system = [
            SystemMsg(
                name="system",
                content=await self._get_system_prompt(),
            ),
        ]
        if self.state.summary:
            msgs_system.append(UserMsg("user", self.state.summary))

        instruction_msgs: list[Msg] = []
        if instructions is not None:
            instruction_msgs.append(
                AssistantMsg(
                    name=self.name,
                    content=[instructions],
                ),
            )

        # Same time text as the runtime injection; keeps other braces intact
        timezone = self.injection_config.timezone
        now = datetime.now(_resolve_timezone(timezone))
        compression_prompt = cfg.compression_prompt.replace(
            "{current_time}",
            f"{now.strftime(self.injection_config.time_format)} ({timezone})",
        )

        messages = (
            msgs_system
            + msgs_to_compress
            + instruction_msgs
            + [
                UserMsg(name="user", content=compression_prompt),
            ]
        )

        # The compression prompt may exceed the context length, here we mark
        # the overflow by a bool flag
        compression_tool_schema = [
            {
                "type": "function",
                "function": {
                    "name": "generate_structured_output",
                    "description": "Call this function to generate "
                    "structured output required by "
                    "the user.",
                    "parameters": cfg.summary_schema,
                },
            },
        ]
        context_overflow = False
        estimated_compression_tokens = await self.model.count_tokens(
            messages,
            compression_tool_schema,
        )
        if estimated_compression_tokens > self.model.context_size:
            logger.warning(
                "The current context length exceeds the model's context "
                "length (%d tokens), the compression maybe failed due to "
                "insufficient reserved context for compression.",
                self.model.context_size,
            )
            context_overflow = True

        # Compress the messages
        res = None
        try:
            res = await self.model.generate_structured_output(
                messages=messages,
                structured_model=cfg.summary_schema,
            )

        except Exception as error:
            if context_overflow:
                logger.warning(
                    "Failed to compress context, which may be caused by "
                    "insufficient reserved context for compression. "
                    "Trying to compress by removing the oldest context.",
                )
                for i in range(1, len(msgs_to_compress) + 1):
                    messages = (
                        msgs_system
                        + msgs_to_compress[i:]
                        + instruction_msgs
                        + [
                            UserMsg(
                                name="user",
                                content=compression_prompt,
                            ),
                        ]
                    )
                    estimated_compression_tokens = (
                        await self.model.count_tokens(
                            messages,
                            compression_tool_schema,
                        )
                    )
                    # Considering trigger_ratio <= 0.9, at least reserve 10%
                    # tokens for compression response
                    if (
                        estimated_compression_tokens
                        < self.model.context_size * cfg.trigger_ratio
                    ):
                        break

                try:
                    res = await self.model.generate_structured_output(
                        messages=messages,
                        structured_model=cfg.summary_schema,
                    )
                except Exception as retry_error:
                    error = retry_error

            if res is None:
                if not cfg.compression_fallback_to_truncation:
                    raise error
                logger.warning(
                    "[AGENT %s]: Summary generation failed: %s. "
                    "Falling back to context truncation.",
                    self.name,
                    error,
                )

        if res is not None and (
            res.finished_reason == FinishedReason.INTERRUPTED
        ):
            logger.warning(
                "The context compression was interrupted and skipped. ",
            )
            raise asyncio.CancelledError()

        # Update the summary
        async def _apply_change() -> None:
            """Apply the context change with interruption protection."""
            if res is not None:
                new_summary = cfg.summary_template.format(**res.content)
            else:
                # Keep the previous summary if the compression failed
                new_summary = self.state.summary or (
                    "<system-info>Some earlier messages were truncated for "
                    "limited context.</system-info>"
                )

            # Offload the compressed context if offloader is provided
            if self.offloader:
                path = await self.offloader.offload_context(
                    self.state.session_id,
                    msgs=msgs_to_compress,
                )
                offload_reminder = (
                    f"<system-reminder>The compressed context"
                    f" is offloaded to '{path}', you can refer"
                    f" to it when needed.</system-reminder>"
                )
                # Avoid duplicating the reminder when the previous summary
                # is reused after a failed compression
                if isinstance(new_summary, list):
                    if not any(
                        isinstance(block, TextBlock)
                        and block.text == offload_reminder
                        for block in new_summary
                    ):
                        new_summary = [
                            *new_summary,
                            TextBlock(text=offload_reminder),
                        ]
                elif offload_reminder not in new_summary:
                    new_summary += f"\n{offload_reminder}"

            # Clear the read tool cache
            await self._clear_unreserved_read_cache(msgs_to_reserve)

            # The current reply msg may be fully compressed, so keep its usage
            current_reply_usage = self._get_reply_usage()

            # Update the context and summary
            self.state.summary = new_summary
            self.state.context = msgs_to_reserve

            if (
                current_reply_usage is not None
                and self._get_reply_usage() is None
            ):
                self.state.append_context(self.name, [])
                self.state.context[-1].usage = current_reply_usage

            # The compression call is not covered by the model call events,
            # so record its cost on the context tail to keep it in the token
            # accounting
            if res is not None and res.usage is not None:
                if not self.state.context:
                    # The whole context is compressed, so carry the cost by an
                    # empty message, which is skipped by the formatters
                    self.state.append_context(self.name, [])

                self.state.context[-1].append_usage(
                    Usage(
                        input_tokens=res.usage.input_tokens,
                        output_tokens=res.usage.output_tokens,
                        cache_input_tokens=res.usage.cache_input_tokens or 0,
                        cache_creation_input_tokens=(
                            res.usage.cache_creation_input_tokens or 0
                        ),
                    ),
                )

            logger.info(
                "[AGENT %s]: The context compression finished.",
                self.name,
            )

        apply_task = asyncio.create_task(_apply_change())
        try:
            await asyncio.shield(apply_task)
        except asyncio.CancelledError:
            await apply_task
            raise

    async def _limit_context_images(self, cfg: ContextConfig) -> None:
        """Limit the number of images in the context according to
        ``cfg.max_image_num``. The oldest images exceeding the limit are
        offloaded to the workspace (if an offloader is provided) and replaced
        by a hint recording the offloaded path; otherwise they are dropped and
        replaced by a hint without path information.

        Image data blocks at the top level of a non-user message are
        replaced by :class:`HintBlock`, while those in user messages or
        nested inside a :class:`ToolResultBlock` / :class:`HintBlock` are
        replaced by :class:`TextBlock` (as required by their type
        constraints).

        Args:
            cfg (`ContextConfig`):
                The context config that provides ``max_image_num``.
        """
        max_image_num = cfg.max_image_num

        def _is_image(block: Any) -> bool:
            """Check whether the given block is an image data block."""
            return isinstance(
                block,
                DataBlock,
            ) and block.source.media_type.startswith("image/")

        # Collect all the image data blocks in chronological order, recorded
        # as (container list, index, block, is_top_level, role)
        images: list[tuple[list, int, DataBlock, bool, str]] = []
        for msg in self.state.context:
            for i, block in enumerate(msg.content):
                if _is_image(block):
                    images.append((msg.content, i, block, True, msg.role))
                elif isinstance(block, ToolResultBlock) and isinstance(
                    block.output,
                    list,
                ):
                    for j, sub in enumerate(block.output):
                        if _is_image(sub):
                            images.append(
                                (block.output, j, sub, False, msg.role),
                            )
                elif isinstance(block, HintBlock) and isinstance(
                    block.hint,
                    list,
                ):
                    for j, sub in enumerate(block.hint):
                        if _is_image(sub):
                            images.append(
                                (block.hint, j, sub, False, msg.role),
                            )

        n_exceed = len(images) - max_image_num
        if n_exceed <= 0:
            return

        logger.info(
            "[AGENT %s]: The number of images in context (%d) exceeds the "
            "limit (%d), removing the oldest %d image(s).",
            self.name,
            len(images),
            max_image_num,
            n_exceed,
        )

        # Offload the images first (which may involve I/O), and then apply
        # the replacements together to avoid partial modification on
        # interruption
        replacements: list[tuple[list, int, HintBlock | TextBlock]] = []
        for container, idx, block, is_top_level, role in images[:n_exceed]:
            url = ""
            if isinstance(block.source, URLSource):
                url = str(block.source.url)
            elif self.offloader is not None:
                saved = await self.offloader.offload_data_block(block)
                if isinstance(saved.source, URLSource):
                    url = str(saved.source.url)

            name = f"named '{block.name}' " if block.name else ""
            if url:
                text = (
                    f"<system-reminder>The image {name}is offloaded into "
                    f"{url}, you can refer to it when needed."
                    f"</system-reminder>"
                )
            else:
                text = (
                    f"<system-reminder>The image {name}is removed to free "
                    f"up context space.</system-reminder>"
                )

            new_block: HintBlock | TextBlock
            if is_top_level and role != "user":
                new_block = HintBlock(hint=text)
            else:
                # User messages only accept text/data blocks (see the
                # validator in Msg), and a hint block is equivalent to a user
                # text block for the model anyway.
                new_block = TextBlock(type="text", text=text)
            replacements.append((container, idx, new_block))

        for container, idx, new_block in replacements:
            container[idx] = new_block

    # ======================================================================
    # Agent core methods, including _reply, _reasoning, _acting, etc.
    # ======================================================================

    async def _reply(
        self,
        inputs: Msg
        | list[Msg]
        | UserConfirmResultEvent
        | UserInterruptEvent
        | ExternalExecutionResultEvent
        | None = None,
        structured_schema: Type[BaseModel] | None = None,
    ) -> AsyncGenerator[AgentEvent | Msg, None]:
        """Reply entry point (maybe wrapped by middleware). The reply loop
        exits only after its ``ReplyEndEvent`` escapes the middleware chain,
        so an ``on_reply`` middleware can swallow the event (receive it
        without yielding) to force another reasoning-acting round."""
        if not self._reply_middlewares:
            agen = self._reply_impl(
                inputs=inputs,
                structured_schema=structured_schema,
            )
        else:

            async def execute_chain(
                index: int = 0,
                inputs: Msg
                | list[Msg]
                | UserConfirmResultEvent
                | UserInterruptEvent
                | ExternalExecutionResultEvent
                | None = inputs,
                structured_schema: Type[BaseModel] | None = structured_schema,
            ) -> AsyncGenerator[AgentEvent | Msg, None]:
                if index >= len(self._reply_middlewares):
                    async for item in self._reply_impl(
                        inputs=inputs,
                        structured_schema=structured_schema,
                    ):
                        yield item
                else:
                    mw = self._reply_middlewares[index]
                    input_kwargs = {
                        "inputs": inputs,
                        "structured_schema": structured_schema,
                    }

                    async def next_handler(
                        **kwargs: Any,
                    ) -> AsyncGenerator[AgentEvent | Msg, None]:
                        async for item in execute_chain(
                            index + 1,
                            **{**input_kwargs, **kwargs},
                        ):
                            yield item

                    async for item in mw.on_reply(
                        agent=self,
                        input_kwargs=input_kwargs,
                        next_handler=next_handler,
                    ):
                        yield item

            agen = execute_chain()

        self._receive_reply_end = False
        async for item in agen:
            # Set before the yield: the suspended `_reply_impl` checks the
            # flag once resumed by the next pull
            if isinstance(item, ReplyEndEvent):
                self._receive_reply_end = True
            yield item

    async def _close_unfinished_tool_calls(
        self,
    ) -> AsyncGenerator[
        ToolResultStartEvent | ToolResultTextDeltaEvent | ToolResultEndEvent,
        None,
    ]:
        """Close the unfinished tool calls on interruption, so the next
        input will be handled normally."""
        if not self.state.context:
            return

        last_msg = self.state.context[-1]
        if last_msg.role != "assistant" or last_msg.name != self.name:
            return

        # Searching for tool calls that requires user confirmation or external
        # execution without tool results
        awaiting_tool_calls: dict = {}
        for index, block in enumerate(last_msg.content):
            if isinstance(block, ToolCallBlock):
                awaiting_tool_calls[block.id] = index
            elif isinstance(block, ToolResultBlock):
                awaiting_tool_calls.pop(block.id, None)

        interruption_message = (
            "<system-reminder>The tool call has been interrupted by "
            "the user.</system-reminder>"
        )

        for index in awaiting_tool_calls.values():
            call_block = last_msg.content[index]
            assert isinstance(call_block, ToolCallBlock)

            # ALLOWED calls are running and SUBMITTED external calls are
            # awaiting their result; both already emitted START.
            if call_block.state not in (
                ToolCallState.ALLOWED,
                ToolCallState.SUBMITTED,
            ):
                yield ToolResultStartEvent(
                    reply_id=self.state.reply_id,
                    tool_call_id=last_msg.content[index].id,
                    tool_call_name=last_msg.content[index].name,
                )

            call_block.state = ToolCallState.FINISHED
            yield ToolResultTextDeltaEvent(
                reply_id=self.state.reply_id,
                tool_call_id=last_msg.content[index].id,
                delta=interruption_message,
            )
            yield ToolResultEndEvent(
                reply_id=self.state.reply_id,
                tool_call_id=last_msg.content[index].id,
                state=ToolResultState.INTERRUPTED,
            )
            last_msg.content.append(
                ToolResultBlock(
                    id=last_msg.content[index].id,
                    name=last_msg.content[index].name,
                    output=interruption_message,
                    state=ToolResultState.INTERRUPTED,
                ),
            )

    async def _reply_impl(  # pylint: disable=too-many-branches
        self,
        inputs: Msg
        | list[Msg]
       …19209 tokens truncated…  unfinished_tool_call_ids = {
            block.id
            for block in self.state.get_unfinished_tool_calls(self.name)
        }
        for index, msg in enumerate(self.state.context[: msg_index + 1]):
            if any(
                block.id in unfinished_tool_call_ids
                for block in msg.get_content_blocks("tool_call")
            ):
                msg_index = index
                break

        # The msgs that won't exceed the reserved token limit
        msgs_to_compress = self.state.context[:msg_index]
        msgs_to_reserve = self.state.context[msg_index + 1 :]
        boundary_msg = self.state.context[msg_index]

        # Handle the boundary Msg
        boundary_msg_to_compress = deepcopy(boundary_msg)
        boundary_msg_to_reserve = deepcopy(boundary_msg)

        attempt_msg = deepcopy(boundary_msg)

        boundary_msg_content = boundary_msg.get_content_blocks()
        block_index = len(boundary_msg_content) - 1
        while block_index >= 0:
            attempt_msg.content = boundary_msg_content[block_index:]

            try_reserved = system_msg + [attempt_msg] + msgs_to_reserve
            reserved_tokens = await self.model.count_tokens(
                try_reserved,
                tools,
            )
            if reserved_tokens > to_reserved_tokens:
                break
            block_index -= 1

        unfinished_block_indexes = [
            index
            for index, block in enumerate(boundary_msg_content)
            if isinstance(block, ToolCallBlock)
            and block.id in unfinished_tool_call_ids
        ]
        if unfinished_block_indexes:
            block_index = min(block_index, min(unfinished_block_indexes) - 1)

        # Adjust the block_index to avoid splitting tool call and result pairs.
        # Moving the boundary can bring another tool call into the compressed
        # part while leaving its result reserved, so repeat until it is stable.
        while True:
            # Check if the reserved part has tool results that don't have the
            # corresponding tool calls
            remain_result_ids = {}
            for i in range(
                len(boundary_msg_content) - 1,
                block_index,
                -1,
            ):
                block = boundary_msg_content[i]
                if isinstance(block, ToolResultBlock):
                    remain_result_ids[block.id] = i
                elif isinstance(block, ToolCallBlock):
                    remain_result_ids.pop(block.id, None)

            # All tool result blocks in the reserved part are paired.
            if not remain_result_ids:
                break

            # Move unmatched results into the compressed part and recheck,
            # because this move can split another tool call/result pair.
            new_block_index = max(remain_result_ids.values())

            if unfinished_block_indexes and new_block_index >= min(
                unfinished_block_indexes,
            ):
                # The move would compress an unfinished tool call, so reserve
                # the calls of the unmatched results instead
                block_index = (
                    min(
                        index
                        for index, block in enumerate(boundary_msg_content)
                        if isinstance(block, ToolCallBlock)
                        and block.id in remain_result_ids
                    )
                    - 1
                )
            else:
                block_index = new_block_index

        # Split the boundary msg content
        boundary_msg_to_compress.content = boundary_msg_content[
            : block_index + 1
        ]
        boundary_msg_to_reserve.content = boundary_msg_content[
            block_index + 1 :
        ]

        if len(boundary_msg_to_compress.content) > 0:
            msgs_to_compress += [boundary_msg_to_compress]

        if len(boundary_msg_to_reserve.content) > 0:
            msgs_to_reserve = [boundary_msg_to_reserve] + msgs_to_reserve

        return msgs_to_compress, msgs_to_reserve

    async def _clear_unreserved_read_cache(
        self,
        msgs_to_reserve: list[Msg],
    ) -> None:
        """Clean Read caches not referenced by reserved Read tool calls."""
        reserved_paths: set[str] = set()
        for msg in msgs_to_reserve:
            for block in msg.get_content_blocks("tool_call"):
                if not (
                    isinstance(block, ToolCallBlock) and block.name == "Read"
                ):
                    continue

                try:
                    tool_input = _json_loads_with_repair(block.input)
                except Exception:  # pylint: disable=broad-exception-caught
                    continue

                file_path = tool_input.get("file_path")
                if isinstance(file_path, str):
                    reserved_paths.add(file_path)

        await self.state.tool_context.clean_file_cache(
            reserved_file_paths=reserved_paths,
        )

    async def _split_tool_result_for_compression(
        self,
        tool_result: ToolResultBlock,
    ) -> tuple[ToolResultBlock, ToolResultBlock | None]:
        """Split the tool result for compression.

        Args:
            tool_result (`ToolResultBlock`):
                The tool result block.

        Returns:
            `tuple[ToolResultBlock, ToolResultBlock | None]`:
                A tuple of the tool result blocks to reserved in context and
                to offload (if any).
        """
        n_tokens = await self.model.count_tokens(
            [AssistantMsg(self.name, content=tool_result.output)],
            None,
        )

        # Return the tool result without truncation
        if n_tokens <= self.context_config.tool_result_limit:
            return tool_result, None

        # Use a copied block for token counting
        copied_tool_result = deepcopy(tool_result)

        # Normalized into content blocks
        if isinstance(copied_tool_result.output, str):
            copied_tool_result.output = [
                TextBlock(text=copied_tool_result.output),
            ]

        # Find the index of the block that will exceed the limit
        boundary_index = 0
        for i in range(len(copied_tool_result.output) - 1, 0, -1):
            copied_tool_result.output = tool_result.output[:i]
            cur_tokens = await self.model.count_tokens(
                [
                    AssistantMsg(
                        self.name,
                        content=copied_tool_result.output,
                    ),
                ],
                None,
            )
            if cur_tokens < self.context_config.tool_result_limit:
                boundary_index = i
                break

        # The blocks to reserve and offload (deep copy to avoid
        # modifying original)
        reserved_blocks: list = [
            deepcopy(b) for b in tool_result.output[:boundary_index]
        ]
        offload_blocks: list = [
            deepcopy(b) for b in tool_result.output[boundary_index + 1 :]
        ]

        # Get the boundary block, if text block, we can truncate it
        boundary_block = tool_result.output[boundary_index]
        if isinstance(boundary_block, TextBlock):
            # Truncate it
            truncated_text = boundary_block.text
            cur_tokens = await self.model.count_tokens(
                [AssistantMsg(self.name, content=reserved_blocks)],
                None,
            )
            cur_tokens_plus = await self.model.count_tokens(
                [
                    AssistantMsg(
                        self.name,
                        content=reserved_blocks + [boundary_block],
                    ),
                ],
                None,
            )
            # Truncate the text by proportion of tokens
            token_delta = cur_tokens_plus - cur_tokens
            remaining_token_budget = (
                self.context_config.tool_result_limit - cur_tokens
            )
            if token_delta <= 0:
                reserved_tokens = (
                    len(truncated_text) if remaining_token_budget > 0 else 0
                )
            else:
                reserved_tokens = int(
                    remaining_token_budget / token_delta * len(truncated_text),
                )
            reserved_tokens = max(
                0,
                min(len(truncated_text), reserved_tokens),
            )

            reserved_text = truncated_text[:reserved_tokens]
            offload_text = truncated_text[reserved_tokens:]

            if reserved_text:
                if (
                    len(reserved_blocks) > 0
                    and reserved_blocks[-1].type == "text"
                ):
                    reserved_blocks[-1].text += reserved_text

                else:
                    reserved_blocks.append(
                        TextBlock(text=reserved_text, id=boundary_block.id),
                    )

            if offload_text:
                if (
                    len(offload_blocks) > 0
                    and offload_blocks[0].type == "text"
                ):
                    offload_blocks[0].text = (
                        offload_text + offload_blocks[0].text
                    )

                else:
                    offload_blocks.insert(
                        0,
                        TextBlock(text=offload_text, id=boundary_block.id),
                    )

        else:
            # Drop the boundary block if inseparable
            offload_blocks.insert(0, boundary_block)

        if len(offload_blocks) == 0:
            return tool_result, None

        # Create new ToolResultBlock instances for reserved and offload
        reserved_tool_result = ToolResultBlock(
            id=tool_result.id,
            name=tool_result.name,
            output=reserved_blocks,
            state=tool_result.state,
            metadata=deepcopy(tool_result.metadata),
            created_at=tool_result.created_at,
            finished_at=tool_result.finished_at,
        )
        offload_tool_result = ToolResultBlock(
            id=tool_result.id,
            name=tool_result.name,
            output=offload_blocks,
            state=tool_result.state,
            metadata=deepcopy(tool_result.metadata),
            created_at=tool_result.created_at,
            finished_at=tool_result.finished_at,
        )

        return reserved_tool_result, offload_tool_result

    # ======================================================================
    # Agent internal utility methods
    # ======================================================================
    async def _get_system_prompt(self) -> str:
        """Get the system prompt of the agent."""
        prompt = [self._system_prompt]

        # Skill related instructions
        skill_instructions = await self.toolkit.get_skill_instructions(
            self.state.tool_context.activated_groups,
        )
        if skill_instructions:
            prompt.append(skill_instructions)

        # Workspace & offloader instructions
        if isinstance(self.offloader, WorkspaceBase):
            offload_instructions = await self.offloader.get_instructions()
            if offload_instructions:
                prompt.append(offload_instructions)

        result = "\n".join(prompt)

        # Apply system_prompt middlewares sequentially (transformer pattern)
        for mw in self._system_prompt_middlewares:
            result = await mw.on_system_prompt(self, result)

        return result

    async def _prepare_model_input(self) -> dict[str, Any]:
        """A unified method to prepare the chat model input according to
        the current context.

        Returns:
            `dict[str, Any]`
                The keyword arguments passed to the model.
        """
        # The system prompt
        messages = [
            SystemMsg(name="system", content=await self._get_system_prompt()),
        ]
        # The compressed summary
        if self.state.summary:
            messages.append(
                UserMsg(name="user", content=self.state.summary),
            )
        # The conversation context
        messages.extend(self.state.context)

        # Equip the compression tool, whose registration is kept across
        # replies so that its schema is stable for prompt caching
        if self.context_config.compression_tool_enabled and (
            await self.toolkit.get_tool(_COMPRESSION_TOOL_NAME)
            is not self._compression_tool
        ):
            await self.toolkit.add_tool(self._compression_tool)

        # Get the tools schemas
        tools = await self.toolkit.get_tool_schemas(
            self.state.tool_context.activated_groups,
        )

        return {
            "messages": messages,
            "tools": tools,
        }

    async def _call_model(
        self,
        messages: list[Msg],
        tools: list[dict],
        tool_choice: ToolChoice | None = None,
    ) -> ChatResponse | AsyncGenerator[ChatResponse, None]:
        """Perform model inference with retry logic and middleware support.

        Args:
            messages (`list[Msg]`):
                The input messages to the model.
            tools (`list[dict]`):
                The function schemas of the tools.
            tool_choice (`ToolChoice | None`, optional):
                The tool choice strategy for the model call.

        Returns:
            `ChatResponse | AsyncGenerator[ChatResponse, None]`:
                The model response, which can be a `ChatResponse` for
                non-streaming models, or an async generator yielding
                `ChatResponse` chunks for streaming models.
        """
        models = [self.model]

        # Fallback to the secondary model if the primary model fails after
        # retries
        if self.model_config.fallback_model:
            models.append(self.model_config.fallback_model)

        last_exception = None
        # ``max_retries`` is the number of retries on top of the initial
        # call (mirrors ``ChatModelBase.max_retries``), so total attempts
        # per model is ``max_retries + 1``.
        for index, model in enumerate(models):
            if index > 0:
                logger.info(
                    "Fallback to model '%s'",
                    model.model,
                )

            for attempt in range(self.model_config.max_retries + 1):
                try:
                    # Apply middleware to wrap the actual model() call
                    if not self._model_call_middlewares:
                        return await model(
                            messages=messages,
                            tools=tools,
                            tool_choice=tool_choice,
                        )
                    else:
                        # pylint: disable=cell-var-from-loop
                        async def execute_chain(
                            index: int = 0,
                            current_model: ChatModelBase = model,
                            messages: list[Msg] = messages,
                            tools: list[dict] = tools,
                            tool_choice: ToolChoice = tool_choice,
                        ) -> ChatResponse | AsyncGenerator[ChatResponse, None]:
                            """Execute the model chain."""
                            if index >= len(self._model_call_middlewares):
                                return await current_model(
                                    messages=messages,
                                    tools=tools,
                                    tool_choice=tool_choice,
                                )
                            else:
                                mw = self._model_call_middlewares[index]
                                input_kwargs = {
                                    "current_model": current_model,
                                    "messages": messages,
                                    "tools": tools,
                                    "tool_choice": tool_choice,
                                }

                                async def next_handler(
                                    **kwargs: Any,
                                ) -> (
                                    ChatResponse
                                    | AsyncGenerator[ChatResponse, None]
                                ):
                                    # pylint: disable=cell-var-from-loop
                                    return await execute_chain(
                                        index + 1,
                                        **{**input_kwargs, **kwargs},
                                    )

                                return await mw.on_model_call(
                                    agent=self,
                                    input_kwargs=input_kwargs,
                                    next_handler=next_handler,
                                )

                        return await execute_chain()
                except Exception as e:
                    last_exception = e
                    # Only log a "Retrying" message when there's actually a
                    # next attempt left for this model. When ``max_retries=0``
                    # or the last retry has been used, the outer loop either
                    # falls over to the fallback or raises.
                    if attempt < self.model_config.max_retries:
                        logger.warning(
                            "Model %s call failed for agent %s. "
                            "Retrying (%d/%d)...",
                            model.model,
                            self.name,
                            attempt + 1,
                            self.model_config.max_retries,
                        )
                    else:
                        logger.warning(
                            "Model %s exhausted all %d attempt(s) "
                            "for agent %s.",
                            model.model,
                            self.model_config.max_retries + 1,
                            self.name,
                        )

        if last_exception:
            raise last_exception from None

        raise RuntimeError(
            "Model call failed after retries, but no exception was raised.",
        )

    def _update_tool_call_state(
        self,
        tool_call_id: str,
        state: ToolCallState,
    ) -> None:
        """Update the tool call state. This function is to avoid the update
        not reflected in the context due to the shallow copy of the content
        blocks somewhere in the code.

        Args:
            tool_call_id (`str`):
                The tool call id to be updated.
            state (`ToolCallState`):
                The new state of the tool call.
        """
        if len(self.state.context) == 0:
            return
        last_msg = self.state.context[-1]
        if last_msg.role != "assistant" or last_msg.name != self.name:
            return
        for block in last_msg.get_content_blocks():
            if isinstance(block, ToolCallBlock) and block.id == tool_call_id:
                block.state = state
                break

    def _save_to_context(
        self,
        blocks: Sequence[
            TextBlock
            | ThinkingBlock
            | ToolCallBlock
            | ToolResultBlock
            | DataBlock
        ],
        usage: ChatUsage | None = None,
    ) -> None:
        """Save content blocks into the context.

        Newly created :class:`AssistantMsg` uses ``self.state.reply_id`` as
        its id so that one reply corresponds to one message and the message
        id matches the ``reply_id`` carried by streaming events.
        """
        msg_usage = (
            Usage(
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_input_tokens=usage.cache_input_tokens or 0,
                cache_creation_input_tokens=(
                    usage.cache_creation_input_tokens or 0
                ),
            )
            if usage is not None
            else None
        )

        # Assistant-produced audio (e.g. qwen-omni speaking aloud) is delivered
        # to the user via streaming events; the raw bytes don't belong in
        # conversation memory. Filtering here keeps every downstream walker
        # (formatter, count_tokens, persistence) honest without each having
        # to remember.
        persisted_blocks = [
            b
            for b in blocks
            if not (
                isinstance(b, DataBlock)
                and isinstance(b.source, (Base64Source, URLSource))
                and b.source.media_type.startswith("audio/")
            )
        ]
        if not persisted_blocks and msg_usage is None:
            return

        self.state.append_context(self.name, persisted_blocks)

        if msg_usage is not None:
            self.state.context[-1].append_usage(msg_usage)

    def _get_last_msg(self) -> Msg | None:
        """Get the last message in the context that belongs to this agent."""
        if len(self.state.context) == 0:
            return None
        last_msg = self.state.context[-1]
        if last_msg.role == "assistant" and last_msg.name == self.name:
            return last_msg
        return None

    def _get_reply_usage(self) -> Usage | None:
        """Get a copy of the accumulated usage for the current reply."""
        last_msg = self._get_last_msg()
        if (
            last_msg is None
            or last_msg.id != self.state.reply_id
            or last_msg.usage is None
        ):
            return None
        return last_msg.usage.model_copy()

    def _next_action(
        self,
        final_msg: Msg | None = None,
    ) -> Reasoning | Acting | Exit:
        """Decide the next action from the current state. Read-only: all
        side effects are performed by the caller ``_reply_impl``."""

        # ===========================================================
        # Step 1: Check executable and awaiting tool calls
        # ===========================================================
        awaiting_tool_calls = self.state.get_awaiting_tool_calls(self.name)

        last_msg = self._get_last_msg()
        if last_msg is not None:
            # In case wrong tool call state, first filter with the results
            finished_ids = {
                _.id for _ in last_msg.get_content_blocks("tool_result")
            }
            # With awaiting tool calls, PENDING ones are blocked (e.g.
            # deduplicated in a batch) and wait; only ALLOWED ones execute
            executable_tool_calls = [
                _
                for _ in last_msg.get_content_blocks("tool_call")
                if _.id not in finished_ids
                and (
                    _.state == ToolCallState.ALLOWED
                    or (
                        _.state == ToolCallState.PENDING
                        and not awaiting_tool_calls
                    )
                )
            ]
            if executable_tool_calls:
                # Next execute the tool calls
                return Acting(tool_calls=executable_tool_calls)

        if awaiting_tool_calls:
            # Next wait for the permission or external execution to finish
            return Exit(
                # The reply doesn't finish yet
                exit_events=None,
                exit_msg=AssistantMsg(
                    id=self.state.reply_id,
                    name=self.name,
                    # both confirmation or external execution
                    content="I'm waiting for your permission or the "
                    "external execution to finish.",
                ),
            )

        # ===========================================================
        # Step 2: Check structured output if no blocked tool calls
        # ===========================================================

        # Structured output requirement and satisfaction
        required = self.state.reply_context.structured_schema is not None
        satisfied = self.state.reply_context.structured_output is not None

        if required and satisfied:
            # Next return the structured output and finish the reply
            return Exit(
                exit_events=[
                    ReplyEndEvent(
                        session_id=self.state.session_id,
                        reply_id=self.state.reply_id,
                        finished_reason=ReplyFinishedReason.COMPLETED,
                    ),
                ],
                exit_msg=AssistantMsg(
                    id=self.state.reply_id,
                    name=self.name,
                    content="The required structured output is generated.",
                    usage=self._get_reply_usage(),
                    finished_reason=ReplyFinishedReason.COMPLETED,
                    structured_output=deepcopy(
                        self.state.reply_context.structured_output,
                    ),
                ),
            )

        elif required and not satisfied:
            # Maybe the model needs further reasoning-acting to
            # generate the structured output
            tool_choice = None
            suffix = (
                "Call it when you are ready to generate the final "
                "structured output."
            )

            # Allow extra grace iterations for structured generation
            if (
                self.state.cur_iter
                >= self.react_config.max_iters
                + self.react_config.structured_output_grace_iters
            ):
                # Deprecated but still emitted for backward compatibility;
                # suppressed since the warning targets consumers
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    exceed_event = ExceedMaxItersEvent(
                        reply_id=self.state.reply_id,
                        name=self.name,
                    )
                return Exit(
                    exit_events=[
                        exceed_event,
                        ReplyEndEvent(
                            session_id=self.state.session_id,
                            reply_id=self.state.reply_id,
                            finished_reason=(
                                ReplyFinishedReason.EXCEED_MAX_ITERS
                            ),
                        ),
                    ],
                    exit_msg=AssistantMsg(
                        id=self.state.reply_id,
                        name=self.name,
                        content="The maximum reasoning-acting iterations "
                        "are exceeded.",
                        usage=self._get_reply_usage(),
                        finished_reason=ReplyFinishedReason.EXCEED_MAX_ITERS,
                    ),
                )

            if self.state.cur_iter >= self.react_config.max_iters:
                # Must call the structured output tool and return the
                # structured output
                tool_choice = ToolChoice(
                    mode=_GenerateStructuredOutput.name,
                )
                suffix = (
                    "You have reached the maximum reasoning-acting "
                    "iterations, so call this tool at once to generate "
                    "the final structured output."
                )

            # Next continue reasoning with injected hint
            return Reasoning(
                hint=HintBlock(
                    hint=[
                        TextBlock(
                            text=(
                                "<system-reminder>You're required to "
                                "generate structured output by calling the "
                                f"'{_GenerateStructuredOutput.name}' tool. "
                                f"{suffix}</system-reminder>"
                            ),
                        ),
                    ],
                    source='{"label": "System", "sublabel": '
                    '"Structured Output Requirement"}',
                ),
                tool_choice=tool_choice,
            )

        # ===========================================================
        # Step 3: Exit with the final message, or continue reasoning
        # ===========================================================

        # The last reasoning produced a text-only final message
        if final_msg is not None:
            # In the normal flow, ``cur_iter == max_iters + 1`` here means
            # this text came from the one forced finalization call.
            exceeded_max_iters = (
                self.state.cur_iter > self.react_config.max_iters
            )
            finished_reason = (
                ReplyFinishedReason.EXCEED_MAX_ITERS
                if exceeded_max_iters
                else ReplyFinishedReason.COMPLETED
            )
            exit_events: list[AgentEvent] = []

            if exceeded_max_iters:
                logger.warning(
                    "Agent %s exceeds the max iteration numbers %d. "
                    "Stop the react loop.",
                    self.name,
                    self.react_config.max_iters,
                )
                final_msg.finished_reason = finished_reason
                # Deprecated but still emitted for backward compatibility;
                # suppressed since the warning targets consumers
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    exit_events.append(
                        ExceedMaxItersEvent(
                            reply_id=self.state.reply_id,
                            name=self.name,
                        ),
                    )

            exit_events.append(
                ReplyEndEvent(
                    session_id=self.state.session_id,
                    reply_id=self.state.reply_id,
                    finished_reason=finished_reason,
                ),
            )
            return Exit(
                exit_events=exit_events,
                exit_msg=final_msg,
            )

        # At equality, the regular iteration budget is exhausted, but the
        # one forced finalization call has not run yet.
        if self.state.cur_iter == self.react_config.max_iters:
            return Reasoning(
                hint=HintBlock(
                    hint=(
                        f"<system-reminder>You have reached the maximum of "
                        f"{self.react_config.max_iters} reasoning-acting "
                        f"iterations. Summarize the work and findings so far "
                        f"and return the final answer as text. Do not call "
                        f"any tools.</system-reminder>"
                    ),
                    source='{"label": "System", "sublabel": '
                    '"Max Iterations Reached"}',
                ),
                tool_choice=ToolChoice(mode="none"),
            )

        # Equality returned above, so reaching this check means the forced
        # finalization call also failed to produce a final message.
        if self.state.cur_iter >= self.react_config.max_iters:
            logger.warning(
                "Agent %s exceeds the max iteration numbers %d. "
                "Stop the react loop.",
                self.name,
                self.react_config.max_iters,
            )

            # Deprecated but still emitted for backward compatibility;
            # suppressed since the warning targets consumers
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                exceed_event = ExceedMaxItersEvent(
                    reply_id=self.state.reply_id,
                    name=self.name,
                )
            return Exit(
                exit_events=[
                    exceed_event,
                    ReplyEndEvent(
                        session_id=self.state.session_id,
                        reply_id=self.state.reply_id,
                        finished_reason=ReplyFinishedReason.EXCEED_MAX_ITERS,
                    ),
                ],
                exit_msg=AssistantMsg(
                    id=self.state.reply_id,
                    name=self.name,
                    content="The maximum reasoning-acting iterations are "
                    "exceeded.",
                    usage=self._get_reply_usage(),
                    finished_reason=ReplyFinishedReason.EXCEED_MAX_ITERS,
                ),
            )

        # By default, continue reasoning
        return Reasoning()

    async def _convert_chat_response_to_event(
        self,
        block_ids: dict,
        chunk: ChatResponse,
    ) -> AsyncGenerator:
        """Convert a ChatResponse chunk into a sequence of agent events. To
        keep the identifiers of the content blocks reasonable, the input
        blocks_ids is used to track the block ids.

        Args:
            block_ids (`dict`):
                The block ids used to track the block generation.
            chunk (`ChatResponse`):
                The chat response chunk to be converted.
        """

        # Classify the content blocks into different types
        text_blocks, thinking_blocks, tool_call_blocks = [], [], []
        data_blocks: list = []
        for block in chunk.content:
            if isinstance(block, TextBlock):
                text_blocks.append(block)
            elif isinstance(block, ThinkingBlock):
                thinking_blocks.append(block)
            elif isinstance(block, ToolCallBlock):
                tool_call_blocks.append(block)
            elif isinstance(block, DataBlock):
                data_blocks.append(block)

        # Handle the thinking stream: continue/open or close.
        # We only auto-close when the chunk also carries no data blocks;
        # a data-only chunk (e.g. an omni-style audio PCM delta) must keep
        # both text and thinking streams alive so the frontend doesn't
        # fragment one logical stream into many separate bubbles.
        if thinking_blocks:
            # Generate a new thinking block id and start event
            if not block_ids.get("thinking"):
                block_ids["thinking"] = _generate_id()
                yield ThinkingBlockStartEvent(
                    reply_id=self.state.reply_id,
                    block_id=block_ids["thinking"],
                )
            # Generate the thinking delta event with the existing id
            yield ThinkingBlockDeltaEvent(
                reply_id=self.state.reply_id,
                block_id=block_ids["thinking"],
                delta="".join([_.thinking for _ in thinking_blocks]),
            )

        elif block_ids.get("thinking") and not data_blocks:
            yield ThinkingBlockEndEvent(
                reply_id=self.state.reply_id,
                block_id=block_ids["thinking"],
            )
            block_ids["thinking"] = None

        # Handle the text stream: continue/open or close.  Placed after
        # thinking so that a chunk carrying both ThinkingBlock and TextBlock
        # emits thinking events first.
        if text_blocks:
            if not block_ids.get("text"):
                block_ids["text"] = _generate_id()
                yield TextBlockStartEvent(
                    reply_id=self.state.reply_id,
                    block_id=block_ids["text"],
                )
            yield TextBlockDeltaEvent(
                reply_id=self.state.reply_id,
                block_id=block_ids["text"],
                delta="".join([_.text for _ in text_blocks]),
            )
        elif block_ids.get("text") and not data_blocks:
            yield TextBlockEndEvent(
                reply_id=self.state.reply_id,
                block_id=block_ids["text"],
            )
            block_ids["text"] = None

        # Handle the tool calls that exist in the current chunk
        for tool_call in tool_call_blocks:
            # Not in previous chunk, start with a start event
            if tool_call.id not in block_ids["tools"]:
                block_ids["tools"].append(tool_call.id)
                yield ToolCallStartEvent(
                    reply_id=self.state.reply_id,
                    tool_call_id=tool_call.id,
                    tool_call_name=tool_call.name,
                )
            yield ToolCallDeltaEvent(
                reply_id=self.state.reply_id,
                tool_call_id=tool_call.id,
                delta=tool_call.input,
            )

        # Handle the tool calls that exist in the previous chunk but not in the
        # current chunk
        finished_ids = set(block_ids["tools"]) - set(
            _.id for _ in tool_call_blocks
        )
        for finished_id in finished_ids:
            yield ToolCallEndEvent(
                reply_id=self.state.reply_id,
                tool_call_id=finished_id,
            )
            block_ids["tools"].remove(finished_id)

        # Handle the data blocks (streaming binary content, e.g. omni audio).
        # Each DataBlock chunk from the model carries a delta payload with a
        # stable block id; we open a stream the first time we see an id and
        # emit delta events for subsequent chunks with the same id.
        for data_block in data_blocks:
            if not isinstance(data_block.source, Base64Source):
                # Only Base64Source carries inline delta bytes; URLSource is
                # one-shot and not part of the streaming protocol.
                continue
            if data_block.id not in block_ids["data"]:
                block_ids["data"].append(data_block.id)
                yield DataBlockStartEvent(
                    reply_id=self.state.reply_id,
                    block_id=data_block.id,
                    media_type=data_block.source.media_type,
                )
            yield DataBlockDeltaEvent(
                reply_id=self.state.reply_id,
                block_id=data_block.id,
                data=data_block.source.data,
                media_type=data_block.source.media_type,
            )

    async def _convert_tool_chunk_to_event(
        self,
        tool_call_id: str,
        output_blocks: str | List[TextBlock | DataBlock],
    ) -> AsyncGenerator:
        """Convert a ToolChunk into a sequence of agent events."""
        if isinstance(output_blocks, str):
            yield ToolResultTextDeltaEvent(
                reply_id=self.state.reply_id,
                tool_call_id=tool_call_id,
                delta=output_blocks,
            )
            return

        for block in output_blocks:
            if isinstance(block, TextBlock):
                yield ToolResultTextDeltaEvent(
                    reply_id=self.state.reply_id,
                    tool_call_id=tool_call_id,
                    delta=block.text,
                )

            elif isinstance(block, DataBlock):
                if isinstance(block.source, Base64Source):
                    yield ToolResultDataDeltaEvent(
                        reply_id=self.state.reply_id,
                        tool_call_id=tool_call_id,
                        media_type=block.source.media_type,
                        data=block.source.data,
                    )
                elif isinstance(block.source, URLSource):
                    yield ToolResultDataDeltaEvent(
                        reply_id=self.state.reply_id,
                        tool_call_id=tool_call_id,
                        media_type=block.source.media_type,
                        url=str(block.source.url),
                    )
