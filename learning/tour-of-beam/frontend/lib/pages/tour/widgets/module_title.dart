/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';

import '../../../models/module.dart';

class ModuleTitleWidget extends StatelessWidget {
  final ModuleModel module;
  final VoidCallback onTap;

  const ModuleTitleWidget({
    required this.module,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return ClickableWidget(
      onTap: onTap,
      child: Padding(
        padding: const EdgeInsets.only(bottom: BeamSizes.size6),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Text(
              module.title,
              style: Theme.of(context).textTheme.headlineMedium,
            ),
            Padding(
              padding: const EdgeInsets.only(right: BeamSizes.size4),
              child: ComplexityWidget(complexity: module.complexity),
            ),
          ],
        ),
      ),
    );
  }
}
