# -*- coding:utf-8 -*-
"""
ms-swift 集成客户端：封装训练与部署能力
"""

import os
from typing import Any, Dict, List, Optional, Tuple, Union

from swift.llm import (
    get_model_tokenizer,
    get_template,
    load_dataset,
    EncodePreprocessor,
)
from swift.tuners import Swift, LoraConfig
from swift.trainers import Seq2SeqTrainer, Seq2SeqTrainingArguments
from swift.utils import (
    get_logger,
    find_all_linears,
    get_model_parameter_info,
    seed_everything,
)


logger = get_logger()


class MsSwiftClient:
    def __init__(
        self,
        output_dir: Optional[str] = None,
        default_system_prompt: str = "You are a helpful assistant.",
        data_seed: int = 42,
    ) -> None:
        seed_everything(data_seed)
        self.output_dir = os.path.abspath(output_dir or "output")
        self.default_system_prompt = default_system_prompt
        self.data_seed = data_seed

    def finetune(
        self,
        model_id_or_path: str,
        dataset: Union[str, List[str]],
        system_prompt: Optional[str] = None,
        max_length: int = 2048,
        lora_rank: int = 8,
        lora_alpha: int = 32,
        gradient_accumulation_steps: int = 16,
        num_train_epochs: int = 1,
        save_steps: int = 50,
        eval_steps: int = 50,
        learning_rate: float = 1e-4,
        split_dataset_ratio: float = 0.01,
        num_proc: int = 4,
        model_name: Optional[List[str]] = None,
        model_author: Optional[List[str]] = None,
        # 新增数据与验证集参数
        val_dataset: Optional[Union[str, List[str]]] = None,
        dataset_num_proc: Optional[int] = None,
        # 训练控制
        save_total_limit: int = 2,
        save_strategy: str = "steps",
        eval_strategy: str = "steps",
        per_device_train_batch_size: int = 1,
        per_device_eval_batch_size: int = 1,
        warmup_ratio: float = 0.05,
        weight_decay: float = 0.1,
        logging_steps: int = 5,
        report_to: Optional[List[str]] = None,
        resume_from_checkpoint: Optional[str] = None,
        metric_for_best_model: Optional[str] = "loss",
        dataloader_num_workers: Optional[int] = None,
        # LoRA/调优
        target_modules: Optional[List[str]] = None,
        # 透传
        extra_trainer_args: Optional[Dict[str, Any]] = None,
        extra_dataset_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行 LoRA 微调，返回输出目录与最近的 checkpoint 路径。"""
        os.makedirs(self.output_dir, exist_ok=True)

        model, tokenizer = get_model_tokenizer(model_id_or_path)
        logger.info(f"model_info: {getattr(model, 'model_info', None)}")

        template = get_template(
            model.model_meta.template,
            tokenizer,
            default_system=system_prompt or self.default_system_prompt,
            max_length=max_length,
        )
        template.set_mode("train")

        target_modules = target_modules or find_all_linears(model)
        lora_config = LoraConfig(
            task_type="CAUSAL_LM",
            r=lora_rank,
            lora_alpha=lora_alpha,
            target_modules=target_modules,
        )
        model = Swift.prepare_model(model, lora_config)
        logger.info(f"lora_config: {lora_config}")
        logger.info(f"model_parameter_info: {get_model_parameter_info(model)}")

        # 数据集加载与编码
        ds_num_proc = dataset_num_proc if dataset_num_proc is not None else num_proc
        # 加载训练/验证集：若显式提供val_dataset则忽略split比例
        if val_dataset is None:
            train_dataset, val_ds = load_dataset(
                dataset,
                split_dataset_ratio=split_dataset_ratio,
                num_proc=ds_num_proc,
                model_name=model_name,
                model_author=model_author,
                seed=self.data_seed,
                **(extra_dataset_args or {}),
            )
        else:
            train_dataset, _ = load_dataset(
                dataset,
                split_dataset_ratio=0.0,
                num_proc=ds_num_proc,
                model_name=model_name,
                model_author=model_author,
                seed=self.data_seed,
                **(extra_dataset_args or {}),
            )
            # 独立加载验证集（与 ms-swift 接口保持一致用法：允许 list 或 str）
            val_ds, _ = load_dataset(
                val_dataset,
                split_dataset_ratio=0.0,
                num_proc=ds_num_proc,
                model_name=model_name,
                model_author=model_author,
                seed=self.data_seed,
                **(extra_dataset_args or {}),
            )
        train_dataset = EncodePreprocessor(template=template)(train_dataset, num_proc=ds_num_proc)
        val_dataset = EncodePreprocessor(template=template)(val_ds, num_proc=ds_num_proc)

        # dataloader_num_workers: Windows 默认为0，其它平台为1；用户可覆盖
        if dataloader_num_workers is None:
            try:
                import platform
                dataloader_num_workers = 0 if platform.system().lower().startswith("win") else 1
            except Exception:
                dataloader_num_workers = 1

        report_to_backends = report_to if report_to is not None else ["tensorboard"]

        training_args = Seq2SeqTrainingArguments(
            output_dir=self.output_dir,
            learning_rate=learning_rate,
            per_device_train_batch_size=per_device_train_batch_size,
            per_device_eval_batch_size=per_device_eval_batch_size,
            gradient_checkpointing=True,
            weight_decay=weight_decay,
            lr_scheduler_type="cosine",
            warmup_ratio=warmup_ratio,
            report_to=report_to_backends,
            logging_first_step=True,
            save_strategy=save_strategy,
            save_steps=save_steps,
            eval_strategy=eval_strategy,
            eval_steps=eval_steps,
            gradient_accumulation_steps=gradient_accumulation_steps,
            num_train_epochs=num_train_epochs,
            metric_for_best_model=metric_for_best_model,
            save_total_limit=save_total_limit,
            logging_steps=logging_steps,
            dataloader_num_workers=dataloader_num_workers,
            data_seed=self.data_seed,
            **(extra_trainer_args or {}),
        )

        model.enable_input_require_grads()
        trainer = Seq2SeqTrainer(
            model=model,
            args=training_args,
            data_collator=template.data_collator,
            train_dataset=train_dataset,
            eval_dataset=val_dataset,
            template=template,
        )
        trainer.train(resume_from_checkpoint=resume_from_checkpoint)

        last_model_checkpoint = getattr(trainer.state, "last_model_checkpoint", None)
        logger.info(f"last_model_checkpoint: {last_model_checkpoint}")
        return {
            "status": "success",
            "output_dir": self.output_dir,
            "last_model_checkpoint": last_model_checkpoint,
        }
